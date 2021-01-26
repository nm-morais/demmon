#!/usr/bin/env python3

import argparse
import matplotlib as mpl
import networkx as nx
import json
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_folder", help="the log file")
    parser.add_argument("output_folder", help="the output path file")
    args = parser.parse_args()
    return args.log_folder, args.output_folder


def parse_lines(lines, node_ip, latencies_added, latencies):
    parent_ip = ""
    node_level = -1
    landmark = False

    for aux in reversed(lines):
        line = aux.strip()

        if "I am landmark" in line:
            landmark = True
        if "My parent changed" in line and parent_ip == "" and line != "":
            split = line.split(" ")
            split_line = line.split(" ")
            parent_name = str(split_line[10])
            parent_name = parent_name.split(":")[0]
            parent_ip = parent_name[6:]

        if "My level" in line and node_level == -1:
            node_level = int(line.split(" ")[-1][:-2])

        if "Latency:" in line and "[NodeWatcher]" in line:
            if "Lowest Latency Peer" in line:
                continue

            split = line.split(" ")
            ip_port = str(split[6])[:-1]
            ip = str(ip_port.split(":")[0])[6:]
            latStr = split[10]
            latStr2 = latStr[:-1]

            try:
                added = latencies_added[(node_ip, ip)]
            except KeyError:
                try:
                    added = latencies_added[(ip, node_ip)]
                except KeyError:
                    latencies.append(
                        (node_ip, ip, (int(latStr2) / 1000000) / 2))
                    latencies_added[(node_ip, ip)] = {}

    return landmark, parent_ip, node_level


def parse_file(file_path, latencies_added):

    latencies = []

    f = open(file_path, "r")
    node_name = str(file_path.split("/")[-2])
    node_ip = node_name.split(":")[0][6:]

    lines = f.readlines()

    print(file_path, "lines:{}".format(len(lines)))

    landmark, parent_ip, node_level = parse_lines(
        lines, node_ip, latencies_added, latencies)

    return landmark, node_ip, parent_ip, node_level, latencies


def parse_files(file_paths, output_folder):

    minLat = 10000000000000000
    maxLat = -1

    level_width = 700
    landmarks = 0
    nodes = {}
    max_level = -1
    parent_less_nodes = 0
    latencies_added = {}

    for file_path in file_paths:
        landmark, node_ip, parent_ip, node_level, latencies = parse_file(
            file_path, latencies_added)
        max_level = max(node_level, max_level)
        if landmark:
            xPos = (landmarks + 1) * level_width / 2
            print("landmark: {}".format(node_ip))
            print("landmark xpos: {}".format(xPos))
            landmarks += 1
            nodes[node_ip] = {
                "node_level": 0,
                "parent": parent_ip,
                "latencies": latencies,
                "pos": [xPos, 0],
                "landmark": landmark
            }
            continue

        if node_level == -1:
            nodes[node_ip] = {
                "node_level": node_level,
                "parent": parent_ip,
                "latencies": latencies,
                "pos": [parent_less_nodes, -3],
                "landmark": landmark
            }
            parent_less_nodes += 100
            continue

        nodes[node_ip] = {
            "node_level": node_level,
            "parent": parent_ip,
            "latencies": latencies,
            "landmark": landmark,
            "pos": [0, 0],
        }

    print(max_level)
    children = {}
    children_counter = {}
    for node in nodes:
        if nodes[node]["node_level"] != -1 and nodes[node]["node_level"] != 0:
            parent = nodes[node]["parent"]
            try:
                children[parent] = children[parent] + 1
            except KeyError:
                children[parent] = 1

    parent_edges = []
    latencyEdges = {}
    latencyEdgeLabels = {}
    nodeLabels = {}
    pos = {}

    for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):
        print("doing node: {} {}".format(
            node, json.dumps(nodes[node], indent=4, sort_keys=True))
        )

        if node.startswith("."):
            continue

        if nodes[node]["node_level"] == -1:
            parent_less_nodes += 100
            pos[node] = nodes[node]["pos"]
            print("err: {} has no parent".format(node))

        if nodes[node]["landmark"]:
            pos[node] = (nodes[node]["pos"][0], nodes[node]["pos"][1])
            nodes[node]["node_level_step"] = level_width
        else:
            parentId = nodes[node]["parent"]
            parent = ""
            try:
                parent = nodes[parentId]
            except KeyError:
                parentPos = (parent_less_nodes, -2)
                parent_less_nodes += 100
                print("err: {} has parent but parent not in node list".format(node))
                continue

            parentPos = parent["pos"]
            parent_children = children[parentId]
            curr_children = 0
            try:
                curr_children = children_counter[parentId]
                children_counter[parentId] += 1
            except:
                children_counter[parentId] = 1

            try:
                thisLvlWidth = nodes[parentId]["node_level_step"] * 0.9
            except:
                print("Node {} has no node_level_step key".format(parentId))
                continue

            thisLvlStep = float(thisLvlWidth) / \
                float(max(parent_children - 1, 1))
            nodes[node]["node_level_step"] = thisLvlStep

            nodePos = ()
            if parent_children == 1:
                nodePos = [parentPos[0], parentPos[1] - max_level]
            else:
                nodePos = [(parentPos[0] - (thisLvlWidth / 2)) +
                           (curr_children * thisLvlStep), parentPos[1] - max_level]

            nodes[node]["pos"] = nodePos
            pos[node] = nodePos
            parent_edges.append((node, parentId))

        for latencyPair in nodes[node]["latencies"]:
            latencyEdges[(latencyPair[0], latencyPair[1])
                         ] = int(latencyPair[2])
        nodeLabels[node] = node

    edge_colors = [latencyEdges[l] for l in latencyEdges]

    parent_colors = []
    for p in parent_edges:
        try:
            parent_colors.append(latencyEdges[p])
            latencyEdgeLabels[p] = latencyEdges[p]
        except KeyError:
            try:
                parent_colors.append(latencyEdges[(p[1], p[0])])
                latencyEdgeLabels[(p[1], p[0])] = latencyEdges[(p[1], p[0])]
            except KeyError:
                parent_colors.append(1000000000)
                latencyEdgeLabels[(p[1], p[0])] = "missing"

    with open('{}/parent_edges.txt'.format(output_folder), 'w') as f:
        for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):
            if nodes[node]["node_level"] == 0:
                f.write("{} ".format(node))
        f.write("\n")
        for parent_edge in parent_edges:
            f.write("{} {}\n".format(parent_edge[0], parent_edge[1]))

    # import matplotlib.pyplot as plt
    # latVals = [latencyEdges[l] for l in latencyEdgeLabels]
    # n, bins, patches = plt.hist(latVals, 50, facecolor='green', alpha=0.75)
    # plt.savefig("{}/histogram.png".format(output_folder))

    import matplotlib.pyplot as plt
    G = nx.Graph()
    fig, ax = plt.subplots(figsize=(18, 8))
    fig.tight_layout()
    cmap = plt.cm.rainbow

    print(json.dumps(nodes, indent=4, sort_keys=True))

    nx.draw_networkx_nodes(G, pos, nodelist=nodeLabels,
                           node_size=300, ax=ax, node_shape="o")
    nx.draw_networkx_labels(G, pos, nodeLabels, font_size=6, ax=ax)
    nx.draw_networkx_edges(G, pos, edgelist=parent_edges,
                           edge_color=parent_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52, width=4, ax=ax)
    nx.draw_networkx_edges(G, pos, style='dashed', edgelist=latencyEdges, width=1,
                           alpha=0.5, edge_color=edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52, ax=ax)
    nx.draw_networkx_edge_labels(
        G, pos, latencyEdgeLabels, label_pos=0.33, alpha=0.5, font_size=6, ax=ax)

    cbaxes = fig.add_axes([0.95, 0.05, 0.01, 0.65])
    norm = mpl.colors.Normalize(vmin=minLat, vmax=maxLat)
    mpl.colorbar.ColorbarBase(
        cbaxes, cmap=cmap, norm=norm, orientation='vertical')
    plt.savefig("{}/topology.png".format(output_folder))


def main():
    log_folder, output_folder = parse_args()
    paths = []
    print(os.listdir(log_folder))
    for node_folder in os.listdir(log_folder):
        print(node_folder)
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))

    parse_files(paths, output_folder)


if __name__ == "__main__":
    main()
