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
import urllib.request
from multiprocessing.pool import ThreadPool

import requests
from PIL import Image
from skimage import io

import IO
from api_keys import PIXABAY_API_KEY

PER_PAGE = 200


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
    is_new = False
    try:
        if filename.endswith('.jpg'):
            if not os.path.exists(filename):
                urllib.request.urlretrieve(url, filename)
                _ = io.imread(filename)
                is_new = True
        elif filename.endswith('.png'):
            new_filename = ''.join([filename[:-3], 'jpg'])
            if not os.path.exists(new_filename):
                urllib.request.urlretrieve(url, filename)
                im = Image.open(filename)
                im.save(new_filename, "JPEG")
                os.remove(filename)
                is_new = True
        return idx, True, is_new

    except Exception as e:
        print('\nBad url or image: ', end=' ')
        print(e)
        if os.path.exists(filename):
            print(f'Deleting file {filename}...', end=' ')
            os.remove(filename)
            print('Deleted')
        return idx, False, is_new


def get_image_metadata(meta, curr_labels):

    page = 0

    while page <= 3:
        page += 1
        temp = download_metadata(curr_labels, page=page)

        if isinstance(temp, str):
            print(temp)
            UC.take_a_nap(t=3)
            break

        if temp['total'] == 0:
            print("Nothing found with curr_labels:", curr_labels)
            UC.take_a_nap(t=3)
            break

        for record in temp['hits']:

            top3_tags = tuple([tag.strip(' ') for tag in record['tags'].split(',')])

            if record['id'] in meta:
                if 'top3' in meta[record['id']]:
                    orig_top3 = meta[record['id']]['top3']
                    if orig_top3 != top3_tags:
                        print('Mismatched Tags:')
                        print(orig_top3)
                        print(top3_tags)

            meta[record['id']] = {'top3': top3_tags,
                                  'height': record['webformatHeight'],
                                  'width': record['webformatWidth'],
                                  'webformatURL': record['webformatURL']}

        if temp['totalHits'] <= page * PER_PAGE:
            break

    return meta


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

    for ii, label in enumerate(labels):


        # if ii < 415:
        #     continue

        # if label in ('papillon',):
        #     # TODO: papillon returns thousands of butterflies and breaks the code.
        #     # TODO: Fix later, skip for now.
        #     continue

        UC.take_a_nap()

        print('\n -- Beginning:', ii, label, '-- Credit:', UC, '--')

        image_meta = {idx: meta for idx, meta in labelsdata.items() if label in meta['top3']}

        image_meta = get_image_metadata(image_meta, {label})

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

                if 'top3' not in labelsdata[idx]:
                    labelsdata[idx]['top3'] = record['top3']
                    labels_updated = True
            else:
                # Create the list of image files to download
                url = record['webformatURL']
                filetype = url.split('.')[-1]
                filename = f"{IO.pixabay_image_dir}/{idx}.{filetype}"
                url_filename_list.append((idx, url, filename))

        n_records = len(url_filename_list)

        # Download new images in parallel. Don't set pooling too high.
        # Remember you can only download 5000 images per hour.
        # Actually, I don't think this part counts toward our limit. WM 10/27/17
        # At any rate, more threads will likely draw more attention.

        if n_records > 0:
            n_new = 0
            n_skipped = 0
            n_error = 0
            sys.stdout.write(f"Remain:{n_records:>4} "
                             f"New:{n_new:>4} "
                             f"Skipped:{n_skipped:>3} "
                             f"Errors:{n_error:>3}")
            results = ThreadPool(2).imap_unordered(download_and_save_image, url_filename_list)
            for idx, result, is_new in results:
                if result:
                    labelsdata[idx] = {}
                    labelsdata[idx]['top3'] = image_meta[idx]['top3']
                    labelsdata[idx]['width'] = image_meta[idx]['width']
                    labelsdata[idx]['height'] = image_meta[idx]['height']
                    labels_updated = True
                    if is_new:
                        n_new += 1
                    else:
                        n_skipped += 1
                else:
                    n_error += 1
                n_records -= 1

                sys.stdout.write(f"\rRemain:{n_records:>4} "
                                 f"New:{n_new:>4} "
                                 f"Skipped:{n_skipped:>3} "
                                 f"Errors:{n_error:>3}")

        print("")
        # update the labels file with the new and updated labels.
        if labels_updated:
            IO.update_files(labelsdata, top3=True)

        print(f'label: {label} completed')

    IO.remove_orphaned_images()

    print('done')
