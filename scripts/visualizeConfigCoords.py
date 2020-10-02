#!/usr/bin/env python3

import matplotlib as mpl
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import random as rand
import networkx as nx
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("coords_file", help="the log file")
    parser.add_argument("parent_edges_file", help="the parent edges file")
    args = parser.parse_args()
    return args


def read_conf_file(file_path):
    f = open(file_path, "r")
    node_positions = {}
    for aux in f.readlines():
        line = aux.strip()
        print(line)
        split = line.split(" ")
        node_id = split[0]
        node_x = split[1]
        node_y = split[2]
        node_positions[node_id] = (int(node_x), int(node_y))
    return node_positions

def read_parent_edges_file(file_path):
    f = open(file_path, "r")
    landmarks = []
    parent_edges = []
    for idx, aux in enumerate(f.readlines()):
        line = aux.strip()
        if idx == 0:
            split = line.split(" ")
            landmarks = split
            continue
        split = line.split(" ")
        parent_edges.append((split[0], split[1]))
    return parent_edges, landmarks


def plotGraph(node_ids, parent_edges, landmarks):
    maxLat = -1
    minLat = 10000000000000
    edges = {}
                
    parent_edge_colors = []

    print("min:", minLat,"max:", maxLat)
    print("landmarks", landmarks)
    cmap = plt.cm.rainbow
    node_colors = []
    

    pos = node_ids

    G = nx.DiGraph()
    regNodes = []

    for node in node_ids:
        if node not in landmarks:
            regNodes.append(node)

    nx.draw_networkx_nodes(G,pos, nodelist=regNodes,node_color='b',node_size=100,alpha=1)
    nx.draw_networkx_nodes(G,pos, nodelist=landmarks,node_color='r',node_size=100,alpha=1)

    nx.draw_networkx_labels(G, pos, font_size=7, font_family="sans-serif", font_color="white")

    #nx.draw_networkx_edges(G, pos, arrows=False, style="dotted", edgelist=latencyEdges, width=1, alpha=0.50, edge_color=edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52)
    nx.draw_networkx_edges(G, pos, arrowsize=10, arrowstyle="->", edgelist=parent_edges, width=1, alpha=1)
    plt.axis("off")
    plt.show()


def main():
    args = vars(parse_args())
    node_positions = read_conf_file(args["coords_file"])
    parent_edges, landmarks = read_parent_edges_file(args["parent_edges_file"])
    plotGraph(node_positions, parent_edges, landmarks)


if __name__ == "__main__":
    main()