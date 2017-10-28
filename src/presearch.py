import os
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
import nltk
from nltk.corpus import wordnet as wn
import matplotlib.pyplot as plt

import IO

nltk.data.path.append(IO.data_source_dir+"/nltk_data")
map_clsloc = IO.imagenet_source_dir+'/ILSVRC/devkit/data/map_clsloc.txt'


def get_hypernyms(s):
    return s.hypernyms()


def get_hyponyms(s):
    return s.hyponyms()


# def traverse(graph, start, node):
#     graph.depth[node.name] = node.shortest_path_distance(start)
#     for child in node.hyponyms():
#         graph.add_edge(node.name, child.name)
#         traverse(graph, start, child)
#
#
# def hyponym_graph(start):
#     G = nx.Graph()
#     G.depth = {}
#     traverse(G, start, start)
#     return G
#
#
# def graph_draw(graph):
#     nx.draw_graphviz(graph,
#         node_size=[16 * graph.degree(n) for n in graph],
#         node_color=[graph.depth[n] for n in graph],
#         with_labels=False)
#     plt.show()
#
#
# dog = wn.synset('dog.n.01')
# graph = hyponym_graph(dog)
# nx.draw(graph)
# plt.show()


def closure_graph(synset, fn):
    seen = set()
    graph = nx.DiGraph()

    def recurse(s):
        if not s in seen:
            seen.add(s)
            graph.add_node(s.name)
            for s1 in fn(s):
                graph.add_node(s1.name)
                graph.add_edge(s.name, s1.name)
                recurse(s1)

    recurse(synset)
    return graph


dog = wn.synset('dog.n.01')
graph = closure_graph(dog, get_hypernyms)
gg = dog.closure(get_hypernyms)
labels = {hyper: hyper.name() for hyper in gg}
pos = graphviz_layout(graph)
nx.draw_networkx(graph, pos, labels=labels)
# nx.draw_networkx_labels(graph, pos, labels)
plt.show()

with open(map_clsloc) as ifs:
    classes_temp = ifs.read().strip().split('\n')

imagenet_classes = [kls.split() for kls in classes_temp]
imagenet_classes = {k: v for k, _, v in imagenet_classes}

orig_hypos = []
for wnid, label in imagenet_classes.items():
    offset = wnid.split('n')[-1]
    hypo = wn.of2ss(offset + 'n')
    orig_hypos.append(hypo)

all_hypos = set()
set_hypers = set()
hyper_to_hypo = {}
for orig_hypo in orig_hypos:
    for hyper in orig_hypo.closure(get_hypernyms, depth=1):
        set_hypers.add(hyper)
        for new_hypo in hyper.closure(get_hyponyms, depth=1):
            all_hypos.add(new_hypo)
            hyper_to_hypo.setdefault(hyper, set()).add(new_hypo)

for hyper, hypos in sorted(hyper_to_hypo.items(), key=lambda x: (x[0].max_depth(), -1*len(x[1]))):
    print(hyper.max_depth(), len(hypos), hyper.name())

good_hypos = all_hypos.difference(set_hypers)
good_hypos = good_hypos.difference(orig_hypos)

print(len(hyper_to_hypo))
print(len(set_hypers))
print(len(orig_hypos))
print(len(all_hypos))
print(len(set_hypers.union(all_hypos)))
print(len(set_hypers.intersection(all_hypos)))
print(len(set_hypers.difference(all_hypos)))
print(len(good_hypos))

imagenet_words = []
for hypo in good_hypos:
    words = hypo.lemma_names()
    if 'dog' in words:
        print(hypo.name(), words)
    imagenet_words += words

tallies = IO.read_pixabay_tally_file(hit_limit=1000)
for tag, n_images in tallies.items():
    if tag in imagenet_words:
        print(n_images, tag)
