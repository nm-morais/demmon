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
        print(file_path)
        f = open(file_path, "r")
        node_name = str(file_path.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]
        parent_ip = ""
        node_level = -1
        latencies = []
        for line in reversed(f.readlines()):

            if "parent:" in line and parent_ip == "":
                #print(line)
                parent_name = str(line.split(" ")[-1])[:-2]
                parent_ip = parent_name.split(":")[0][6:]

            if "My level" in line and node_level == -1:
                node_level = int(line.split(" ")[-1][:-2][:-1])
                if node_level > max_level:
                    max_level = node_level

            if "Latency:" in line and "[NodeWatcher]" in line:
                if "Lowest Latency Peer" in line:
                    continue
                split = line.split(" ")
                #print(split[6])

                ip_port = str(split[7])[:-1]
                ip = str(ip_port.split(":")[0])[6:]
                
                latStr = split[13]
                latStr2 = latStr[:-1]
                latencies.append((ip,int(latStr2)))

        if node_level == -1:
            xPos = landmarks * 2500
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

    pos = {}
    landmark_list = []
    parentEdges = []
    latencyEdges = {}
    latencyEdgeLabels = {}
    nodeLabels = {}

    for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):

        if nodes[node]["node_level"] == 0:
            nodeLabels[node] = node
            pos[node] = nodes[node]["pos"]
            try:
                nChildren = children[node]
            except KeyError:
                nChildren = 0
            landmark_list.append(node)

        else:
            nodeLabels[node] = node
            parentId = nodes[node]["parent"]
            parent = nodes[parentId]
            
            parentPos = parent["pos"]

            curr = currChildren[parentId]

            try:
                nChildren = children[node]
            except KeyError:
                nChildren = 0
            try:
                parent_children = children[parentId]
            except KeyError:
                parent_children = 0

            nodePos = [(parentPos[0] - parent_children * 250) +
                       curr * 250, parentPos[1] + 7]

            nodes[node]["pos"] = nodePos
            pos[node] = nodePos
            children[parentId] = children[parentId] - 1
            currChildren[parentId] = currChildren[parentId] + 1
            parentEdges.append((parentId, node))

        for latencyPair in nodes[node]["latencies"]:
            # G.add_edge(node, latencyPair[0], weight=latencyPair[1],
            #           parent=False, latency=True, label=latencyPair[1])
            latencyEdges[(latencyPair[0], node)] = int(
                latencyPair[1] / 1000000)

            latencyEdgeLabels[(latencyPair[0], node)] = str(int(
                latencyPair[1] / 1000000))[:-4]


    '''
    latVals = [latencyEdges[l] for l in latencyEdgeLabels]
    print(latVals)

    n, bins, patches = plt.hist(latVals, 50,facecolor='green', alpha=0.75)
    plt.show()
    '''
    for p in pos:
        aux = int(pos[p][1])
        pos[p][1] = (max_level - aux)

    fig, ax = plt.subplots(figsize=(25, 10))
    fig.tight_layout()

    node_list = [n for n in pos]

    minLat = 10000000000000000
    maxLat = -1


    for latPair in latencyEdges:
        currLatVal = latencyEdges[latPair]
        minLat = min(minLat, currLatVal)
        maxLat = max(maxLat, currLatVal)


    edge_colors = [latencyEdges[l] for l in latencyEdges]
    #print()

    for node in nodes:
        print("{}:{}".format(node, nodes[node]))
    parent_colors = []
    for p in parentEdges:
        try:
            parent_colors.append(latencyEdges[p])
        except KeyError:
            parent_colors.append(latencyEdges[(p[1], p[0])])


    #print(minLat, maxLat)
    #print(latencyEdges)
    #print(edge_colors)

    #pos = nx.spring_layout(node_list, pos=pos, iterations=10000)
    
    cmap = plt.cm.rainbow
    
    nx.draw_networkx_nodes(G, pos, nodelist=node_list,
                           node_size=300, ax=ax, node_shape="o")
    nx.draw_networkx_labels(G, pos, nodeLabels, font_size=6, ax=ax)
    nx.draw_networkx_edges(G, pos, edgelist=parentEdges,
                           edge_color=parent_colors, edge_cmap=cmap, edge_vmin=minLat, edge_vmax=maxLat, width=4, ax=ax)
    nx.draw_networkx_edges(G, pos, edgelist=latencyEdges, width=1,
                           alpha=0.75, edge_color=edge_colors, edge_cmap=cmap, edge_vmin=minLat, edge_vmax=maxLat, ax=ax)
    #nx.draw_networkx_edge_labels(G, pos, latencyEdgeLabels,  label_pos=0.66 , alpha=0.5, font_size=5, ax=ax)
    
    cbaxes = fig.add_axes([0.95, 0.05, 0.01, 0.65]) 
    norm = mpl.colors.Normalize(vmin=minLat, vmax=maxLat)
    cb1 = mpl.colorbar.ColorbarBase(cbaxes, cmap=cmap,norm=norm, orientation='vertical')

    plt.show()

def main():
    log_folder = parse_args()
    paths = []
    for node_folder in os.listdir(log_folder):
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))

    parse_files(paths)

if __name__ == "__main__":
    main()
