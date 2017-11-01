"""

We need to create a new list of words to feed to fetch_pixabay_data.py

1. Read in the Pixabay tallies file.
2. Read in the Pixabay metadata file.
3. Read in the Imagenet1000 file.

Make sure tags with spaces are replaced with underscores so wordnet will catch them.
Use NLTK wordnet to only search on nouns.

"""
from operator import itemgetter

import nltk
from nltk.corpus import wordnet as wn

import IO

nltk.data.path.append(IO.data_source_dir+"/nltk_data")

imagenet_classes = IO.read_imagenet_wnid_words_file()
imagenet_labels = list(imagenet_classes.values())

imagenet_synsets = []
for wnid, label in imagenet_classes.items():
    offset = wnid.split('n')[-1]
    synset = wn.of2ss(offset + 'n')
    imagenet_synsets.append(synset)

p_tallies = IO.read_pixabay_tally_file(hit_limit=0, top3=True)

p_metadata = IO.read_pixabay_metadata_file()

# How many images have 3, 2, 1, and 0 labels from ImageNet?
# How many images have 3, 2, 1, and 0 words from WordNet?

id_tags_dict = {ii: meta['top3'] for ii, meta in p_metadata.items()}

num_images_with_tags_in_imagenet = {0: 0, 1: 0, 2: 0, 3: 0}
for ii, tags in id_tags_dict.items():
    jj = 0
    for tag in tags:
        if tag in imagenet_labels:
            jj += 1
    num_images_with_tags_in_imagenet[jj] += 1

for num_tags, num_images in num_images_with_tags_in_imagenet.items():
    print(num_tags, num_images)


# Get a list of tags not in imagenet, but that occur frequently in p_tallies.
credit = 1000
new_query_tags = []
for tag, tally in sorted(p_tallies.items(), key=itemgetter(1), reverse=True):
    if tag in imagenet_labels:
        continue
    if credit % 50 == 0:
        print(tally, tag)
    if credit <= 0:
        break
    new_query_tags.append(tag)
    credit -= 1

new_query_tag_file = '/home/maddoxw/PycharmProjects/Multi_Label_Pixabay/data/query_tags.txt'
with open(new_query_tag_file, 'w') as ofs:
    for tag in new_query_tags:
        ofs.write(tag + '\n')

# For each label in p_tallies, how many images contain that label?

print('done')

