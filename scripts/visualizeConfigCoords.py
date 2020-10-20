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
    parser.add_argument("output_folder", help="the output folder")
    parser.add_argument("latencies_file", help="the latencies file")
    args = parser.parse_args()
    return args

def read_latencies_file(file_path):
    f = open(file_path, "r")
    node_latencies = []
    for aux in f.readlines():
        line = aux.strip()
        split = line.split(" ")
        node_latencies.append([float(lat) for lat in split])
    return node_latencies

def read_conf_file(file_path):
    f = open(file_path, "r")
    node_positions = {}
    node_ids = []
    for aux in f.readlines():
        line = aux.strip()
        print(line)
        split = line.split(" ")
        node_id = split[0]
        node_x = split[1]
        node_y = split[2]
        node_positions[node_id] = (float(node_x), float(node_y))
        node_ids.append(node_id)
    return node_positions, node_ids

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


def plotGraph(node_positions, node_ids, parent_edges, landmarks, latencies, output_folder):
    maxLat = -1
    minLat = 10000000000000
    edges = {}
                
    parent_edge_colors = []

    print("min:", minLat,"max:", maxLat)
    print("landmarks", landmarks)
    cmap = plt.cm.rainbow
    node_colors = []
    

    pos = node_positions
    fig, ax = plt.subplots(figsize=(18, 8))
    ax.autoscale(enable=True, axis='both', tight=True)
    fig.tight_layout()
    G = nx.DiGraph()
    regNodes = []
    labels = {}
    pos_higher = {}
    latencyEdges = {}

    for node_idx, node_id in enumerate(node_ids):
        if node_id == "10.28":
            node_latencies = latencies[node_idx]
            for lat_idx, lat in enumerate(node_latencies): 

                if lat_idx == node_idx:
                    continue
                if lat_idx == len(node_ids):
                    break

                latencyEdges[(node_id, node_ids[lat_idx])] = lat

    for k in node_positions:
        v = node_positions[k]
        if(v[1]>0):
            pos_higher[k] = (v[0]+150, v[1] + 150)
        else:
            pos_higher[k] = (v[0]-150, v[1]- 150)
    
    for node in node_positions:
        if node not in landmarks:
            regNodes.append(node)

    for node in node_positions:
        labels[node] = node

    latencyEdgeLabels = {}
    for l in latencyEdges:
        latencyEdgeLabels[l] = latencyEdges[l]

    nx.draw_networkx_nodes(G,pos, nodelist=regNodes,node_color='b',node_size=35,alpha=1)
    nx.draw_networkx_nodes(G,pos, nodelist=landmarks,node_color='r',node_size=35,alpha=1)
    nx.draw_networkx_labels(G, pos_higher, labels, font_size=7, font_family="sans-serif", font_color="black")
    nx.draw_networkx_edges(G, pos, arrowsize=12,  style='dashed', arrowstyle="->", edgelist=parent_edges, width=1, alpha=1)
    
    # nx.draw_networkx_edge_labels(G, pos, latencyEdgeLabels,  label_pos=0.33 , alpha=1, font_size=8, ax=ax)
    # nx.draw_networkx_edges(G, pos, arrows=False, style="dotted", edgelist=latencyEdges, width=1, alpha=0.25)
    
    plt.axis("off")
    plt.savefig("{}/topology_coords.png".format(output_folder))


# topology_coords.png
def main():
    args = vars(parse_args())
    node_positions, node_ids = read_conf_file(args["coords_file"])
    parent_edges, landmarks = read_parent_edges_file(args["parent_edges_file"])
    latencies = read_latencies_file(args["latencies_file"])
    plotGraph(node_positions, node_ids, parent_edges, landmarks, latencies, args["output_folder"])

if __name__ == "__main__":
    main()