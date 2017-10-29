#!/usr/bin/env python
"""
Get data from pixabay using pixabay api.
make sure you have an account with pixabay
make sure a file named "api_keys.py" exists in
the same folder as this file and that it has the line,

PIXABAY_API_KEY = "YOURAPIKEYGOESHERE"

where YOURAPIKEYGOESHERE is your personal pixabay api key.
(it's free when you sign up.)
"""
import os
import sys
import time
import urllib
# import urllib.error
# import urllib.parse
# import urllib.request
from operator import itemgetter
from multiprocessing.pool import ThreadPool

import requests
from PIL import Image
from skimage import io

import IO
import utils
from api_keys import PIXABAY_API_KEY

PER_PAGE = 200
PULL_PERCENTAGE = 0.93


class UserCredit(object):
    """
    Keep track of our Pixabay API request RateLimit.
    In this class I refer to requests as credits.
    If we use up all our credits, we'll have to wait for the RateLimit to reset
    """

    def __init__(self):
        self.time_remaining = 1800  # seconds
        self.credits_remaining = 2500  # num requests

    def __call__(self, headers):
        self.time_remaining = int(headers['X-RateLimit-Reset'])

        credit = int(headers['X-RateLimit-Remaining'])
        # Lets
        if self.credits_remaining - credit != 1:
            print('WARNING: Credit mismatch: before {}, after {}'.format(self.credits_remaining, credit))
        self.credits_remaining = credit

    def __str__(self):
        return f"{self.credits_remaining}"

    def take_a_nap(self, t=None):
        """
        Forcefully suspend all execution for (self.time_remaining + 1) seconds.
        Note: I'm adding a second here to account for any possible round off error.
        This will only happen if you are out of credits.
        (i.e. if you've hit your pixabay api rate limit)
        """
        # Short circuit override to force a nap.
        if t is not None:
            seconds = t
            credit = 0
        else:
            seconds = self.time_remaining + 1
            credit = self.credits_remaining

        if credit <= 1:
            for i in range(seconds, 0, -1):
                sys.stdout.write("\rSleeping: " + str(i) + ' ' + 'z'*(3 - i % 3))
                time.sleep(1)
            print("")


UC = UserCredit()


def download_metadata(labels, page=1, per_page=PER_PAGE):
    """

    Returns
    -------
    A python dict / json
    """
    base_url = 'https://pixabay.com/api/'
    query = {'key': PIXABAY_API_KEY,
             'q': '+'.join(labels),
             'image_type': 'photo',
             # 'order': 'latest',
             'per_page': str(per_page),
             'page': str(page)}
    query_list = ['='.join([k, v]) for k, v in query.items()]
    query_string = '&'.join(query_list)
    url = '?'.join([base_url, query_string])
    response = requests.get(url, headers={'content-type': 'application/json'})
    UC(response.headers)
    return response.json() if response.status_code == 200 else response.content


def download_and_save_image(args_tuple):
    idx, url, filename = args_tuple
    try:
        if filename.endswith('.jpg'):
            if not os.path.exists(filename):
                urllib.request.urlretrieve(url, filename)
                _ = io.imread(filename)
        elif filename.endswith('.png'):
            new_filename = ''.join([filename[:-3], 'jpg'])
            if not os.path.exists(new_filename):
                urllib.request.urlretrieve(url, filename)
                im = Image.open(filename)
                im.save(new_filename, "JPEG")
                os.remove(filename)
        return idx, True

    except Exception as e:
        print('Bad url or image')
        print(e)
        if os.path.exists(filename):
            print(f'Deleting file {filename}...', end=' ')
            os.remove(filename)
            print('Deleted')
        return idx, False


def filter_labels(old_tags):
    new_tags = set()
    for tag in list(old_tags):
        if tag.startswith('the '):
            tag = tag[4:]
        if tag in PIXABAY_ALIASES:
            tag = PIXABAY_ALIASES[tag]

        if tag in BLACKLIST or tag in BLACKLIST_LOWER:
            continue
        elif tag[-1] == 's' and tag[:-1] in BLACKLIST:
            continue
        elif tag[-1] == 's' and tag[:-1] in BLACKLIST_LOWER:
            continue
        elif tag[-2:] == 'es' and tag[:-2] in BLACKLIST:
            continue
        elif tag[-3:] == 'ies' and tag[:-3] + 'y' in BLACKLIST:
            continue
        elif tag[-3:] == 'ing' and tag[:-3] in BLACKLIST:
            continue
        elif tag in WHITELIST or tag in WHITELIST_LOWER:
            new_tags.add(tag)
            continue
        elif tag[-1] == 's' and tag[:-1] in WHITELIST:
            continue
        elif tag[-1] == 's' and tag[:-1] in WHITELIST_LOWER:
            continue
        elif tag[-2:] == 'es' and tag[:-2] in WHITELIST:
            continue
        elif tag[-3:] == 'ies' and tag[:-3] + 'y' in WHITELIST:
            continue
        elif tag[-3:] == 'ing' and tag[:-3] in WHITELIST:
            continue
        else:
            new_tags.add(tag)

    return new_tags


def update_used_labels(curr_labels, image_ids):

    for used_label_group, old_image_ids in iter(USED_LABELS.items()):
        if used_label_group.issubset(curr_labels):
            USED_LABELS[used_label_group].update(image_ids)
        # if used_label_group.issuperset(curr_labels):
        #     image_ids.update(USED_LABELS[used_label_group])

    USED_LABELS[frozenset(curr_labels)] = image_ids


def get_image_metadata(meta, curr_labels):

    total = 0
    n_hits = 0
    n_new = 0
    n_updated = 0
    page = 0
    image_ids = set()

    while page <= 3:
        page += 1
        temp = download_metadata(curr_labels, page=page)

        if isinstance(temp, str):
            print(temp)
            print('suspending for 60 seconds...')
            UC.take_a_nap(t=60)
            break

        if temp['total'] == 0:
            print("temp['total'] == 0", "curr_labels:", curr_labels, "page:", page)
            print('suspending for 60 seconds...')
            UC.take_a_nap(t=60)
            break

        total = temp['total']
        for record in temp['hits']:
            n_hits += 1

            top3_tags = tuple([tag.strip(' ') for tag in record['tags'].split(',')])

            grouped_tags = set.union(set(top3_tags), curr_labels)
            if record['id'] in meta:
                orig_top3 = meta[record['id']]['top3']
                if orig_top3 != top3_tags:
                    print('Mismatched Tags:')
                    print(orig_top3)
                    print(top3_tags)
                orig_tags = meta[record['id']]['tags']
                grouped_tags.update(orig_tags)
            else:
                orig_tags = []

            new_tags = filter_labels(grouped_tags)
            image_ids.add(record['id'])

            if len(orig_tags) == 0:
                n_new += 1
            elif len(orig_tags) != len(new_tags):
                n_updated += 1
            else:
                continue

            meta[record['id']] = {'top3': top3_tags,
                                  'tags': new_tags,
                                  'height': record['webformatHeight'],
                                  'width': record['webformatWidth'],
                                  'webformatURL': record['webformatURL']}

        if temp['totalHits'] <= page * PER_PAGE:
            break

    update_used_labels(curr_labels, image_ids)

    assert len(USED_LABELS[frozenset(curr_labels)]) == n_hits
    # print out some logging info.
    print(f'{len(meta):7d} '
          f'{total:6d} '
          f'{total * PULL_PERCENTAGE:6.0f} '
          f'{n_hits:6d} '
          f'{n_updated:5d} '
          f'{n_new:5d}', frozenset(curr_labels))

    return meta, temp['total']


def get_new_label_set(new_image_metadata, curr_labels):

    label_counts = utils.get_counts(new_image_metadata)

    for label_name, _ in sorted(label_counts.items(), key=itemgetter(1), reverse=True):

        if label_name in curr_labels:
            continue

        new_labels = curr_labels.union([label_name])

        if len('+'.join(new_labels)) > 100:
            # pixabay api rule
            continue

        dup_set_found = False
        for used_label_group, image_ids in iter(USED_LABELS.items()):
            if len(used_label_group.symmetric_difference(new_labels)) == 0:
                dup_set_found = True
                break

        if not dup_set_found:
            break
    else:
        new_labels = set()

    return new_labels


def create_image_metadata_subset(curr_metadata, curr_labels):
    new_image_metadata = {}
    for idx, meta in iter(curr_metadata.items()):
        if curr_labels.issubset(meta['tags']):
            new_image_metadata[idx] = meta
    return new_image_metadata


def merge_image_metadata_sets(current_metadata, future_metadata):
    for idx, meta in iter(future_metadata.items()):
        current_metadata[idx] = meta
    return current_metadata


def recurse_labels(current_metadata, current_labels):

    current_metadata, total = get_image_metadata(current_metadata, current_labels)

    while True:

        if len(USED_LABELS[frozenset(current_labels)]) > total * PULL_PERCENTAGE:
            break
        if len(current_metadata) > total * PULL_PERCENTAGE:
            break

        new_image_metadata = create_image_metadata_subset(current_metadata, current_labels)
        new_labels = get_new_label_set(new_image_metadata, current_labels)
        new_image_metadata = recurse_labels(new_image_metadata, new_labels)
        current_metadata = merge_image_metadata_sets(current_metadata, new_image_metadata)

    return current_metadata


PIXABAY_ALIASES = IO.read_pixabay_aliases_file(dual=True)

WHITELIST = IO.get_whitelist_words()
WHITELIST_LOWER = set([w.lower() for w in list(WHITELIST)])

wordnet_nouns = IO.read_wordnet_exc_file("noun")
BLACKLIST = set(wordnet_nouns.keys())
BLACKLIST = IO.read_wordnet_index_file("verb", output=BLACKLIST)
BLACKLIST = IO.read_wordnet_exc_file("verb", output=BLACKLIST)
BLACKLIST = IO.read_wordnet_index_file("adv", output=BLACKLIST)
BLACKLIST = IO.read_wordnet_exc_file("adv", output=BLACKLIST)
BLACKLIST = IO.read_wordnet_index_file("adj", output=BLACKLIST)
BLACKLIST = IO.read_wordnet_exc_file("adj", output=BLACKLIST)
BLACKLIST.difference_update(WHITELIST)
BLACKLIST_LOWER = set([w.lower() for w in list(BLACKLIST)])

USED_LABELS = {}

if __name__ == '__main__':

    map_clsloc = IO.imagenet_source_dir + '/ILSVRC/devkit/data/map_clsloc.txt'
    with open(map_clsloc) as ifs:
        classes_temp = ifs.read().strip().split('\n')

    imagenet_classes = [kls.split() for kls in classes_temp]
    imagenet_classes = {k: v.replace('_', ' ') for k, _, v in imagenet_classes}
    labels = list(imagenet_classes.values())

    # with open('data/planets.names') as ifs:
    #     labels = ifs.read().strip().split('\n')

    IO.merge_orphaned_metadata()
    IO.merge_orphaned_images()

    labelsdata = IO.read_pixabay_metadata_file()

    # new_labelsdata = {}
    # for idx, meta in iter(labelsdata.items()):
    #     new_labelsdata[idx] = {}
    #     new_tags = filter_labels(meta['tags'])
    #     new_labelsdata[idx]['tags'] = new_tags
    #     new_labelsdata[idx]['width'] = meta['width']
    #     new_labelsdata[idx]['height'] = meta['height']
    #     IO.write_pixabay_metadata_file(new_labelsdata)
    # label_counts_new = utils.get_counts(new_labelsdata)
    # IO.write_pixabay_tally_file(label_counts_new)

    for label in labels:

        UC.take_a_nap()

        image_meta = {idx: meta for idx, meta in labelsdata.items() if label in meta['tags']}

        print(' -- Beginning:', label, '--')
        print(f'{"Current":>7s} '
              f'{"total":>6s} '
              f'{"frac":>6s} '
              f'{"hits":>6s} '
              f'{"upd":>5s} '
              f'{"new":>5s}')

        image_meta = recurse_labels(image_meta, {label})

        url_filename_list = []
        labels_updated = False
        for idx, record in iter(image_meta.items()):
            if idx in labelsdata:
                if labelsdata[idx]['width'] != record['width']:
                    if labelsdata[idx]['height'] != record['height']:
                        print('\nIMAGE CHANGED ON PIXABAY!!!')
                        print(f'record: {idx}')
                        print(f'old: {labelsdata}')
                        print(f'new: {record}')
                        print('\n')
                        continue

                # update existing tags
                tags_old = labelsdata[idx]['tags']
                tags_new = record['tags']
                if len(tags_old.intersection(tags_new)) != len(tags_new):
                    tags_new.update(tags_old)
                    labelsdata[idx]['tags'] = tags_new
                    labels_updated = True

            else:
                # Create the list of image files to download
                url = record['webformatURL']
                filetype = url.split('.')[-1]
                filename = f"{IO.pixabay_image_dir}/{idx}.{filetype}"
                url_filename_list.append((idx, url, filename))

        # download new images in parallel
        n_records = len(url_filename_list)
        start = time.time()
        print(f"Remain: {n_records}")
        # don't set pooling too high.  remember you can only download 5000 images per hour.
        # Actually, I don't think this part counts toward our limit. WM 10/27/17
        results = ThreadPool(2).imap_unordered(download_and_save_image, url_filename_list)
        for idx, result in results:
            if result:
                labelsdata[idx] = {}
                labelsdata[idx]['tags'] = image_meta[idx]['tags']
                labelsdata[idx]['width'] = image_meta[idx]['width']
                labelsdata[idx]['height'] = image_meta[idx]['height']
                labels_updated = True
                n_records -= 1
                sys.stdout.write("\rRemain:" + str(n_records) + " Timer:" + str(time.time() - start))
                if labels_updated and n_records % 100 == 0:
                    print("")
                    # print(f"Remain: {n_records}, Timer: {time.time() - start}")
                    start = time.time()

        print("")
        # update the labels file with the new and updated labels.
        if labels_updated:
            IO.update_files(labelsdata)
        print(f'label: {label} completed')

    IO.remove_orphaned_images()

    print('done')
