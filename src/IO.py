import os
import json
import shutil
import pickle
from operator import itemgetter
from PIL import Image
import networkx as nx
import utils

data_source_dir = "/media/Borg_LS/DATA"
imagenet_source_dir = os.path.join(data_source_dir, "imagenet")
wordnet_source_dir = os.path.join(data_source_dir, "nltk_data", "corpora", "wordnet")
pixabay_source_dir = os.path.join(data_source_dir, "pixabay")
pixabay_image_dir = os.path.join(pixabay_source_dir, "JPEGImages")
pixabay_orphans_dir = os.path.join(pixabay_source_dir, "orphans")


def load_txt(filename, col_delim=None):
    with open(filename) as ifs:
        data = ifs.read().strip().split('\n')
    if col_delim is not None:
        words = []
        for line in data:
            words.append(line.split(col_delim))
        data = words
    return data


def load_pkl(filename):
    """ Wrapper for pickle.load() """
    if os.path.exists(filename):
        with open(filename, 'rb') as ifs:
            pkl_obj = pickle.load(ifs)
    else:
        pkl_obj = {}
    return pkl_obj


def dump_pkl(filename, pkl_obj):
    """ Wrapper for pickle.dump() """
    if len(pkl_obj) > 0:
        with open(filename, 'wb') as ofs:
            pickle.dump(pkl_obj, ofs, pickle.HIGHEST_PROTOCOL)
    return


def read_wordnet_hierarchy_file():
    filename = os.path.join(imagenet_source_dir, "wordnet.is_a.txt")
    graph = nx.read_edgelist(filename, create_using=nx.DiGraph())
    assert nx.is_directed_acyclic_graph(graph)
    return graph


def read_wordnet_synset_words_file():
    filename = os.path.join(imagenet_source_dir, "synset_words.txt")
    with open(filename) as ifs:
        lines = ifs.read().strip().split('\n')
    synset_words = [line.split('\t') for line in lines]
    synsets, word_strings = list(zip(*synset_words))
    word_lists = [[w.strip() for w in ws.split(',')] for ws in word_strings]
    synset_words_dict = {synset: word_list for synset, word_list in zip(synsets, word_lists)}
    return synset_words_dict


def get_whitelist_words():
    synset_words_dict = read_wordnet_synset_words_file()
    return set([s for ss in synset_words_dict.values() for s in ss])


def read_wordnet_index_file(part_of_speech, output=None):
    if output is None:
        output = []
    filename = os.path.join(wordnet_source_dir, "index." + part_of_speech)
    with open(filename) as ifs:
        for line in ifs.readlines():
            if line.startswith('  '):
                continue
            data = line.strip().split()[0].replace('_', ' ')
            if isinstance(output, list):
                output.append(data)
            elif isinstance(output, set):
                output.add(data)
    return output


def read_wordnet_exc_file(part_of_speech, output=None, verbose=False):
    if verbose:
        print(part_of_speech)
    filename = os.path.join(wordnet_source_dir, part_of_speech + ".exc")
    with open(filename) as ifs:
        lines = ifs.read().strip().split('\n')
    word_list2 = [l.split() for l in lines]
    if output is None:
        output = {}
    if isinstance(output, dict):
        output = {w[0]: w[1:] for w in word_list2}
    elif isinstance(output, set):
        # output2 = set()
        for word_list in word_list2:
            if verbose and len(word_list) > 2:
                print(word_list)
            for w in word_list:
                output.add(w.replace('_', ' '))
            # for i, w in enumerate(word_list):
            #     if i == 0:
            #         output.add(w.replace('_', ' '))
            #     else:
            #         output2.add(w.replace('_', ' '))
            # output.add(word_list[1:])

    return output


def read_pixabay_metadata_file():
    meta_file = os.path.join(pixabay_source_dir, 'metadata.pkl')
    return load_pkl(meta_file)


def write_pixabay_metadata_file(metadata):
    meta_file = os.path.join(pixabay_source_dir, 'metadata.pkl')
    dump_pkl(meta_file, metadata)


def read_pixabay_orphans_file():
    meta_file = os.path.join(pixabay_source_dir, 'orphans.pkl')
    return load_pkl(meta_file)


def write_pixabay_orphans_file(orphan_metadata):
    meta_file = os.path.join(pixabay_source_dir, 'orphans.pkl')
    dump_pkl(meta_file, orphan_metadata)


def read_pixabay_tally_file(hit_limit=0, top3=False):
    tf = 'tally3.txt' if top3 else 'tally.txt'
    tally_file = os.path.join(pixabay_source_dir, tf)
    with open(tally_file) as ifs:
        lines = ifs.read().strip().split('\n')
    tallies = [line.split('\t') for line in lines]
    tallies = {label: int(tally) for tally, label in tallies if int(tally) > hit_limit}
    return tallies


def write_pixabay_tally_file(label_counts, top3=False):
    tf = 'tally3.txt' if top3 else 'tally.txt'
    tally_file = os.path.join(pixabay_source_dir, tf)
    with open(tally_file, 'w') as ofs:
        for label0, counts in sorted(label_counts.items(), key=itemgetter(1), reverse=True):
            ofs.write(f"{counts}\t{label0}\n")


def read_pixabay_totals_file():
    totals_file = os.path.join(pixabay_source_dir, 'totals.txt')
    if not os.path.exists(totals_file):
        return {}
    with open(totals_file) as ifs:
        lines = ifs.read().strip().split('\n')
    tallies = [line.split('\t') for line in lines]
    tallies = {tuple(query.split(',')): int(total) for total, query in tallies}
    return tallies


def write_pixabay_totals_file(totalsdata):
    totals_file = os.path.join(pixabay_source_dir, 'totals.txt')
    with open(totals_file, 'w') as ofs:
        for query, counts in sorted(totalsdata.items(), key=itemgetter(1), reverse=True):
            ofs.write(f"{counts}\t{','.join(query)}\n")


def read_pixabay_image_blacklist_file():
    imageblacklist_file = os.path.join(pixabay_source_dir, 'image_blacklist.txt')
    if not os.path.exists(imageblacklist_file):
        return set()
    with open(imageblacklist_file) as ifs:
        lines = ifs.read().strip().split('\n')
    imageblacklist = map(int, lines)
    return set(imageblacklist)


def write_pixabay_image_blacklist_file(imageblacklist):
    imageblacklist_file = os.path.join(pixabay_source_dir, 'image_blacklist.txt')
    with open(imageblacklist_file, 'w') as ofs:
        for ii in sorted(imageblacklist):
            ofs.write(f"{ii}\n")


def read_pixabay_aliases_file(dual=False):
    aliases_file = os.path.join(pixabay_source_dir, 'aliases.json')
    with open(aliases_file) as ifs:
        pixabay_aliases = json.load(ifs)
    if dual:
        pixabay_aliases = {alias: label for label, alias_list in pixabay_aliases.items() for alias in alias_list}
    # else:
    #     pixabay_aliases = pixabay_aliases_temp
    return pixabay_aliases


def read_pixabay_blacklist_file():
    blacklist_file = os.path.join(pixabay_source_dir, 'blacklist.txt')
    with open(blacklist_file) as ifs:
        pixabay_blacklist = set(ifs.read().strip().lower().split('\n'))
    return pixabay_blacklist


def remove_orphaned_images():
    metadata = read_pixabay_metadata_file()
    orphaned_files = []
    for image_file in os.listdir(pixabay_image_dir):
        key = int(image_file.split('.')[0])
        if key not in metadata:
            orphaned_files.append(image_file)

    for orphaned_file in orphaned_files:
        source_file = os.path.join(pixabay_image_dir, orphaned_file)
        target_file = os.path.join(pixabay_orphans_dir, orphaned_file)
        shutil.move(source_file, target_file)

    print(f'{len(orphaned_files)} images orphaned')
    return


def merge_orphaned_images():
    for orphaned_file in os.listdir(pixabay_orphans_dir):
        source_file = os.path.join(pixabay_orphans_dir, orphaned_file)
        target_file = os.path.join(pixabay_image_dir, orphaned_file)
        shutil.move(source_file, target_file)


def remove_orphaned_metadata():
    metadata = read_pixabay_metadata_file()
    orphans = read_pixabay_orphans_file()

    dups = list(set.intersection(set(metadata.keys()), set(orphans.keys())))
    for dup in dups:
        del orphans[dup]

    orphaned_keys = []
    for key, meta in iter(metadata.items()):
        filename = f"{pixabay_image_dir}/{key}.{'jpg'}"
        if not os.path.exists(filename):
            orphaned_keys.append(key)

    if len(orphaned_keys) == 0:
        return
    for key in orphaned_keys:
        orphans[key] = metadata.pop(key)

    write_pixabay_orphans_file(orphans)
    write_pixabay_metadata_file(metadata)

    print(f'{len(orphaned_keys)} records orphaned')
    return


def merge_orphaned_metadata():
    metadata = read_pixabay_metadata_file()
    orphans = read_pixabay_orphans_file()

    if len(orphans) == 0:
        return
    for key, orphan in iter(orphans.items()):
        metadata[key] = orphan

    os.remove(os.path.join(pixabay_source_dir, 'orphans.pkl'))
    write_pixabay_metadata_file(metadata)


def update_files(metadata, top3=False):
    write_pixabay_metadata_file(metadata)
    remove_orphaned_metadata()
    metadata = read_pixabay_metadata_file()
    print(f'metadata file saved. {len(metadata)} total records.', end=' ')
    label_counts = utils.get_counts(metadata)
    write_pixabay_tally_file(label_counts, top3=top3)
    print(f'tally file saved. {len(label_counts)} unique labels.', end=' ')


def convert_png_to_jpg():
    for filename in os.listdir(pixabay_image_dir):
        if filename.endswith('.png'):
            new_filename = ''.join([filename[:-3], 'jpg'])
            new_filename = pixabay_image_dir + new_filename
            if os.path.exists(new_filename):
                os.remove(new_filename)
            filename = pixabay_image_dir + filename
            im = Image.open(filename)
            im.save(new_filename, "JPEG")
            os.remove(filename)

