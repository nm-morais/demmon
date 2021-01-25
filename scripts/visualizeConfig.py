#!/usr/bin/env python3

import argparse
import matplotlib.pyplot as plt
import networkx as nx


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="the log file")
    parser.add_argument("latencies_file", help="the latencies file")
    parser.add_argument("parent_edges_file", help="the parent edges file")
    args = parser.parse_args()
    return args


def read_conf_file(file_path):
    f = open(file_path, "r")
    node_ids = []
    for aux in f.readlines():
        line = aux.strip()
        split = line.split(" ")
        # print(line)
        node_ip = split[0]
        identifier = str(node_ip[6:])
        # print(identifier)
        node_ids.append(identifier)

    return node_ids


def read_latencies_file(file_path):
    f = open(file_path, "r")
    node_latencies = []
    for aux in f.readlines():
        line = aux.strip()
        split = line.split(" ")
        node_latencies.append([float(lat) for lat in split])
    return node_latencies


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


def plotGraph(node_ids, latencies, parent_edges, landmarks):
    maxLat = -1
    minLat = 10000000000000
    G = nx.DiGraph()
    edges = {}
    n_nodes = len(node_ids)
    latencyEdges = []

    for node_idx, node_id in enumerate(node_ids):
        node_latencies = latencies[node_idx]
        for lat_idx, lat in enumerate(node_latencies):

            if lat_idx == node_idx:
                continue
            if lat_idx == len(node_ids):
                break

            if lat == 0:
                continue

            print(lat_idx, node_idx, lat)

            minLat = min(minLat, lat)
            maxLat = max(maxLat, lat)
            latencyEdges.append((node_id, node_ids[lat_idx], lat))

    edge_colors = []
    parent_edge_colors = []
    for latEdge in latencyEdges:
        G.add_edge(latEdge[0], latEdge[1], weight=latEdge[2])
        edge_colors.append(latEdge[2])
        if (latEdge[0], latEdge[1]) in parent_edges:
            idx = parent_edges.index((latEdge[0], latEdge[1]))
            parent_edges[idx] = (latEdge[0], latEdge[1], latEdge[2])
            parent_edge_colors.append(latEdge[2])
        if (latEdge[1], latEdge[0]) in parent_edges:
            idx = parent_edges.index((latEdge[1], latEdge[0]))
            parent_edges[idx] = (latEdge[1], latEdge[0], latEdge[2])
            parent_edge_colors.append(latEdge[2])

    print("min:", minLat, "max:", maxLat)
    print("landmarks", landmarks)
    cmap = plt.cm.rainbow
    pos = nx.kamada_kawai_layout(G)
    nodes = G.nodes()
    node_colors = []

    for node in nodes:
        # print(node, landmarks)
        if node in landmarks:
            node_colors.append("g")
        else:
            node_colors.append("black")

    nx.draw_networkx_nodes(G, pos, node_size=500, node_color=node_colors)
    # nx.draw_networkx_nodes(G,pos, nodelist=landmarks,node_color='r',node_size=500,alpha=0.8)
    nx.draw_networkx_labels(G, pos, font_size=7,
                            font_family="sans-serif", font_color="white")
    nx.draw_networkx_edges(G, pos, arrows=False, style="dotted", edgelist=latencyEdges, width=1, alpha=0.50,
                           edge_color=edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52)
    nx.draw_networkx_edges(G, pos, arrowsize=10, arrowstyle="->", edgelist=parent_edges, width=3, alpha=1,
                           edge_color=parent_edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52)
    plt.axis("off")
    plt.show()


def main():
    args = vars(parse_args())
    node_ids = read_conf_file(args["config_file"])
    latencies = read_latencies_file(args["latencies_file"])
    parent_edges, landmarks = read_parent_edges_file(args["parent_edges_file"])
    plotGraph(node_ids, latencies, parent_edges, landmarks)


if __name__ == "__main__":
    main()
