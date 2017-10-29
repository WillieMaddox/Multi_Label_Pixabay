
def iou(a, b):
    return len(a.intersection(b))/len(a.union(b))


def print_class_counts(name, class_counts, skip_zeros=False, mapper=None):
    print(f'--------{name}--------')
    for label, count in iter(class_counts.items()):
        if skip_zeros and count == 0:
            continue
        lbl = mapper[label] if mapper else label
        print(f'{lbl:15s} {count:10d}')
    print('------------------------')


def check_word(word, words_set, suffix=None):
    test_word = word.rstrip(suffix) if suffix and word.endswith(suffix) else word

    iword = words_set.intersection({test_word})
    if len(iword) == 0:
        return 0
    elif len(iword) == 1:
        return 1
    elif len(iword) > 1:
        return 2


def get_counts(md):
    lc = {}
    for record in iter(md.values()):
        if 'top3' in record:
            for tag in record['top3']:
                if tag not in lc:
                    lc[tag] = 0
                lc[tag] += 1
    return lc


def simplify_json(meta_old, keys_to_keep):
    meta_new = {}
    for name, record_old in iter(meta_old.items()):
        record_new = {}
        for key, value in iter(record_old.items()):
            if key in keys_to_keep:
                record_new[key] = value
        meta_new[name] = record_new
    return meta_new


def extract_tags(meta_old):
    meta_new = {}
    for idx, record_old in iter(meta_old.items()):
        meta_new[int(idx)] = record_old['top3']
    return meta_new


