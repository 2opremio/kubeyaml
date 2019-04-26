import sys
import argparse
import functools
import copy
import deepdiff
import collections
from ruamel.yaml import YAML

# The container name, by proclamation, used for an image supplied in a
# FluxHelmRelease
FHR_CONTAINER = 'chart-image'

class NotFound(Exception):
    pass

def parse_args():
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers()

    image = subparsers.add_parser('image', help='update an image ref')
    image.add_argument('--namespace', required=True)
    image.add_argument('--kind', required=True)
    image.add_argument('--name', required=True)
    image.add_argument('--container', required=True)
    image.add_argument('--image', required=True)
    image.add_argument('--patch-mode', required=False, action='store_true', default=False,
                       help='Generate strategic merge patch instead of editing the input')
    image.set_defaults(func=update_image)

    def note(s):
        k, v = s.split('=')
        return k, v

    annotation = subparsers.add_parser('annotate', help='update annotations')
    annotation.add_argument('--namespace', required=True)
    annotation.add_argument('--kind', required=True)
    annotation.add_argument('--name', required=True)
    annotation.add_argument('--patch-mode', required=False, action='store_true', default=False,
                            help='Generate strategic merge patch instead of editing the input')
    annotation.add_argument('notes', nargs='+', type=note)
    annotation.set_defaults(func=update_annotations)

    return p.parse_args()

def yaml():
    y = YAML()
    y.explicit_start = True
    y.explicit_end = False
    y.preserve_quotes = True
    return y

def bail(reason):
        sys.stderr.write(reason); sys.stderr.write('\n')
        sys.exit(2)

class AlwaysFalse(object):
    def __init__(self):
        pass

    def __get__(self, instance, owner):
        return False

    def __set__(self, instance, value):
        pass

def apply_to_yaml(fn, infile, outfile):
    # fn :: iterator a -> iterator b
    y = yaml()
    # Hack to make sure no end-of-document ("...") is ever added
    y.Emitter.open_ended = AlwaysFalse()
    docs = y.load_all(infile)
    y.dump_all(fn(docs), outfile)


def get_smp(doc1, doc2):
    smp = None
    diff = deepdiff.DeepDiff(t1=doc1, t2=doc2, verbose_level=1, view='tree')
    changes = []
    for k in ['values_changed', 'dictionary_item_added', 'dictionary_item_removed']:
        if k in diff:
            changes += diff[k]
    if len(changes) > 0:
        smp = {}
    for c in changes:
        diff_node, smp_node = c.all_up, smp
        while True:
            if isinstance(diff_node.t2, collections.OrderedDict):
                if 'kind' in diff_node.t2:
                    smp_node['kind'] = diff_node.t2['kind']
                    if 'metadata' in diff_node.t2:
                        metadata = {}
                        if 'name' in diff_node.t2['metadata']:
                            metadata['name'] = diff_node.t2['metadata']['name'] 
                        if 'namespace' in diff_node.t2['metadata']:
                            metadata['namespace'] = diff_node.t2['metadata']['namespace']
                        if len(metadata) > 0:
                            smp_node['metadata'] = metadata

            if diff_node.down is None:
                break

            if isinstance(diff_node.down.t2, collections.OrderedDict):
                if diff_node.t2_child_rel.param not in smp_node:
                    # Initialize it if it wasn't
                    smp_node[diff_node.t2_child_rel.param] = {}
                smp_node = smp_node[diff_node.t2_child_rel.param]
            elif isinstance(diff_node.down.t2, list):
                if diff_node.t2_child_rel.param not in smp_node:
                    # Initialize it if it wasn't
                    smp_node[diff_node.t2_child_rel.param] = [None]
                smp_node = smp_node[diff_node.t2_child_rel.param]
            else:
                if diff_node.t2_child_rel is None:
                    # deletion
                    smp_node[diff_node.t1_child_rel.param] = None
                else:
                    smp_node[diff_node.t2_child_rel.param] = diff_node.down.t2

            diff_node = diff_node.down

    return smp


def update_image(args, docs):
    """Update the manifest specified by args, in the stream of docs"""
    found = False
    for doc in docs:
        if not found:
            for m in manifests(doc):
                c = find_container(args, m)
                if c != None:
                    if args.patch_mode:
                        original_doc = copy.deepcopy(doc)
                    set_container_image(m, c, args.image)
                    found = True
                    if args.patch_mode:
                        patch_doc = get_smp(original_doc, doc)
                        yield patch_doc
                    break
        if not args.patch_mode:
            yield doc
    if not found:
        raise NotFound()

def update_annotations(spec, docs):
    def ensure(d, *keys):
        for k in keys:
            try:
                d = d[k]
            except KeyError:
                d[k] = dict()
                d = d[k]
        return d

    found = False
    for doc in docs:
        if not found:
            for m in manifests(doc):
                if match_manifest(spec, m):
                    if spec.patch_mode:
                        original_doc = copy.deepcopy(doc)
                    notes = ensure(m, 'metadata', 'annotations')
                    for k, v in spec.notes:
                        if v == '':
                            try:
                                del notes[k]
                            except KeyError:
                                pass
                        else:
                            notes[k] = v
                    if len(notes) == 0:
                        del m['metadata']['annotations']
                    found = True
                    if spec.patch_mode:                 
                        patch_doc = get_smp(original_doc, doc)
                        yield patch_doc
                    break
        if not spec.patch_mode:
            yield doc
    if not found:
        raise NotFound()

def manifests(doc):
    if doc['kind'] == 'List':
        for m in doc['items']:
            yield m
    else:
        yield doc

def match_manifest(spec, manifest):
    try:
        # NB treat the Kind as case-insensitive
        if manifest['kind'].lower() != spec.kind.lower():
            return False
        if manifest['metadata'].get('namespace', 'default') != spec.namespace:
            return False
        if manifest['metadata']['name'] != spec.name:
            return False
    except KeyError:
        return False
    return True

def podspec(manifest):
    if manifest['kind'] == 'CronJob':
        spec = manifest['spec']['jobTemplate']['spec']['template']['spec']
    else:
        spec = manifest['spec']['template']['spec']
    return spec

def containers(manifest):
    if manifest['kind'] in ['FluxHelmRelease', 'HelmRelease']:
        return fluxhelmrelease_containers(manifest)
    spec = podspec(manifest)
    return spec.get('containers', []) + spec.get('initContainers', [])

def find_container(spec, manifest):
    if not match_manifest(spec, manifest):
        return None
    for c in containers(manifest):
        if c['name'] == spec.container:
            return c
    return None

def set_container_image(manifest, container, image):
    if manifest['kind'] in ['FluxHelmRelease', 'HelmRelease']:
        set_fluxhelmrelease_container(manifest, container, image)
    else:
        container['image'] = image

def mappings(values):
    return ((k, values[k]) for k in values if isinstance(values[k], collections.abc.Mapping))

# There are different ways of interpreting FluxHelmRelease values as
# images, and we have to sniff to see which to use.
def fluxhelmrelease_containers(manifest):
    def get_image(values):
        image = values['image']
        if isinstance(image, collections.abc.Mapping) and 'repository' in image and 'tag' in image:
            values = image
            image = image['repository']
        if 'tag' in values and values['tag'] != '':
            image = '%s:%s' % (image, values['tag'])
        return image

    containers = []
    values = manifest['spec']['values']
    # Easiest one: the values section has a key called `image`, which
    # has the image used somewhere in the templates. Since we don't
    # know which container it appears in, it gets a standard name.
    if 'image' in values:
        containers =  [{
            'name': FHR_CONTAINER,
            'image': get_image(values),
        }]
    # Second easiest: if there's at least one dict in values that has
    # a key `image`, then all such dicts are treated as containers,
    # named for their key.
    for k, v in mappings(values):
        if 'image' in v:
            containers.append({'name': k, 'image': get_image(v)})
    return containers

def set_fluxhelmrelease_container(manifest, container, replace):
    def set_image(values):
        image = values['image']
        imageKey = 'image'

        if isinstance(image, collections.abc.Mapping) and 'repository' in image and 'tag' in image:
            values = image
            imageKey = 'repository'

        if 'tag' in values:
            im, tag = replace, ''
            try:
                segments = replace.split(':')
                if len(segments) == 2:
                    im, tag = segments
                elif len(segments) == 3:
                    im = ':'.join(segments[:2])
                    tag = segments[2]
            except ValueError:
                pass
            values[imageKey] = im
            values['tag'] = tag
        else:
            values[imageKey] = replace

    values = manifest['spec']['values']
    if container['name'] == FHR_CONTAINER and 'image' in values:
        set_image(values)
        return
    for k, v in mappings(values):
        if k == container['name'] and 'image' in v:
            set_image(v)
            return
    raise NotFound

def main():
    args = parse_args()
    try:
        apply_to_yaml(functools.partial(args.func, args), sys.stdin, sys.stdout)
    except NotFound:
        bail("manifest not found")

if __name__ == "__main__":
    main()
