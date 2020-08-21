#!/usr/bin/env python3

import matplotlib.pyplot as plt
import networkx as nx
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_folder", help="the log file")
    args = parser.parse_args()
    return args.log_folder


def parse_files(file_paths):

    landmarks = 0
    G = nx.Graph()
    nodes = {}
    attrs = {}
    max_level = -1

    for file_path in file_paths:
        f = open(file_path, "r")
        node_name = str(file_path.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]
        parent_ip = ""
        node_level = -1
        latencies = []
        for line in reversed(f.readlines()):

            if "My parent" in line and parent_ip == "":
                parent_name = str(line.split(" ")[-1])
                parent_ip = parent_name.split(":")[0][6:]
                #print("parent_ip", parent_ip)
                G.add_edge(parent_ip, node_ip, weight=0.6)

            if "My level" in line and node_level == -1:
                node_level = int(line.split(" ")[-1][:-2])
                if node_level > max_level:
                    max_level = node_level

            if "Latency to" in line:
                if "is higher" in line:
                    continue
                split = line.split(" ")
                ip_port = split[7][6:-1]
                ip = str(ip_port.split(":")[0])
                latency = split[8]
                latencies.append((ip, int(latency)))

        if node_level == -1:
            xPos = landmarks
            landmarks += 1
            yPos = 0
            nodes[node_ip] = {
                "node_level": 0,
                "parent": parent_ip,
                "latencies": latencies,
                "pos": [xPos, yPos]
            }
        else:
            nodes[node_ip] = {
                "node_level": node_level,
                "parent": parent_ip,
                "latencies": latencies,
            }

    children = {}
    currChildren = {}

    for node in nodes:
        if nodes[node]["node_level"] != 0:
            parent = nodes[node]["parent"]
            try:
                children[parent] = children[parent] + 1
            except KeyError:
                children[parent] = 1
                currChildren[parent] = 1

    currLandmarks = 0
    pos = {}
    latencyEdges = []
    parentEdges = []
    latencyEdgeLabels = {}
    nodeLabels = {}

    for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):
        if nodes[node]["node_level"] == 0:
            nodeLabels[node] = node
            pos[node] = nodes[node]["pos"]
        else:
            print(node)
            nodeLabels[node] = node
            parentId = nodes[node]["parent"]
            parent = nodes[parentId]
            parentPos = parent["pos"]

            curr = currChildren[parentId]

            nChildren = children[parentId]

            if curr % 2 == 0:
                nodePos = [parentPos[0] - curr /
                           nChildren / 5, nodes[node]["node_level"]]
            else:
                nodePos = [parentPos[0] + curr /
                           nChildren / 5, nodes[node]["node_level"]]
            nodes[node]["pos"] = nodePos
            pos[node] = nodePos

            print(parentPos[0], curr, nChildren, nodePos)

            children[parentId] = children[parentId] - 1
            currChildren[parentId] = currChildren[parentId] + 1
            parentEdges.append((parentId, node))

        for latencyPair in nodes[node]["latencies"]:
            # G.add_edge(node, latencyPair[0], weight=latencyPair[1],
            #           parent=False, latency=True, label=latencyPair[1])
            latencyEdges.append((node, latencyPair[0]))
            latencyEdgeLabels[(node, latencyPair[0])] = latencyPair[1]

    for p in pos:
        aux = int(pos[p][1])
        pos[p][1] = max_level - aux

    nx.draw_networkx_nodes(G, pos, node_size=2500)
    nx.draw_networkx_labels(G, pos, nodeLabels, font_size=7)
    nx.draw_networkx_edges(G, pos, edgelist=parentEdges, width=2)
    nx.draw_networkx_edge_labels(G, pos, latencyEdgeLabels, font_color='red', label_pos=0.3 , font_size=7)
    nx.draw_networkx_edges(G, pos, edgelist=latencyEdges,
                           width=1, alpha=0.5, edge_color="b", style="dashed")

    plt.axis('off')
    plt.show()


def main():
    log_folder = parse_args()
    paths = []
    for node_folder in os.listdir(log_folder):
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "DemonTree.log":
                paths.append("{}/{}".format(node_path, node_file))

    parse_files(paths)


if __name__ == "__main__":
    main()
