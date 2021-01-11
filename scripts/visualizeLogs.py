#!/usr/bin/env python3

import argparse
import matplotlib as mpl
import matplotlib.pyplot as plt
import networkx as nx
import json
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_folder", help="the log file")
    parser.add_argument("output_folder", help="the output path file")
    args = parser.parse_args()
    return args.log_folder, args.output_folder


def parse_files(file_paths, output_folder):
    level_width = 700
    landmarks = 0
    G = nx.Graph()
    nodes = {}
    max_level = -1

    parent_less_nodes = 0

    for file_path in file_paths:
        f = open(file_path, "r")
        node_name = str(file_path.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]
        parent_ip = ""
        node_level = -1
        landmark = False
        latencies = []
        lines = f.readlines()
        print(file_path, "lines:{}".format(len(lines)))
        latencies_added = {}
        for aux in reversed(lines):
            line = aux.strip()

            if "I am landmark" in line:
                landmark = True
            if "My parent changed" in line and parent_ip == "" and line != "":
                # print(line)

                # print(line.split(" "))
                split = line.split(" ")
                for i, s in enumerate(split):
                    print(i, s)
                split_line = line.split(" ")
                # for i, s in enumerate(split_line):
                #     print(i, s)

                parent_name = str(split_line[10])
                parent_name = parent_name.split(":")[0]
                parent_ip = parent_name[6:]
                # print("Node {} has parent {}".format(node_name, parent_ip))

            if "My level" in line and node_level == -1:
                # print(line)
                node_level = int(line.split(" ")[-1][:-2])
                # print(node_level)
                if node_level > max_level:
                    max_level = node_level

            if "Latency:" in line and "[NodeWatcher]" in line:
                if "Lowest Latency Peer" in line:
                    continue

                split = line.split(" ")
                # print(split)

                ip_port = str(split[6])[:-1]
                ip = str(ip_port.split(":")[0])[6:]

                # for i, s in enumerate(split):
                #     print(i, s)
                # print("ip:", ip)

                # print(line)
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

        # print(latencies_added)
        # print(latencies)

        if landmark:
            xPos = (landmarks + 1) * level_width / 2
            print("landmark: {}".format(node_ip))
            print("landmark xpos: {}".format(xPos))
            landmarks += 1
            yPos = 0
            nodes[node_ip] = {
                "node_level": 0,
                "parent": parent_ip,
                "latencies": latencies,
                "pos": [xPos, yPos],
                "landmark": landmark
            }
        else:
            if node_level == -1:
                nodes[node_ip] = {
                    "node_level": node_level,
                    "parent": parent_ip,
                    "latencies": latencies,
                    "pos": [parent_less_nodes, -3],
                    "landmark": landmark
                }
                parent_less_nodes += 100
            else:
                nodes[node_ip] = {
                    "node_level": node_level,
                    "parent": parent_ip,
                    "latencies": latencies,
                    "landmark": landmark,
                    "pos": [parent_less_nodes, -3],
                }
                parent_less_nodes += 100

    children = {}
    currChildren = {}

    for node in nodes:
        if nodes[node]["node_level"] != 0:
            parent = nodes[node]["parent"]
            try:
                children[parent] = children[parent] + 1
            except KeyError:
                children[parent] = 1
                currChildren[parent] = 0

    pos = {}
    landmark_list = []
    parent_edges = []
    latencyEdges = {}
    latencyEdgeLabels = {}
    nodeLabels = {}

    for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):
        if node.startswith("."):
            continue
        for latencyPair in nodes[node]["latencies"]:
            # G.add_edge(node, latencyPair[0], weight=latencyPair[1],
            #           parent=False, latency=True, label=latencyPair[1])
            latencyEdges[(latencyPair[0], latencyPair[1])
                         ] = int(latencyPair[2])

        if nodes[node]["landmark"]:
            nodeLabels[node] = node
            pos[node] = nodes[node]["pos"]
            try:
                nChildren = children[node]
            except KeyError:
                nChildren = 0
            landmark_list.append(node)
        else:
            nodeLabels[node] = node
            if nodes[node]["parent"] != "":
                parentId = nodes[node]["parent"]
                parent = nodes[parentId]
                parentPos = parent["pos"]

                parent_children = children[parentId]
                lvl = nodes[node]["node_level"]

                # nodePos = [(parentPos[0] - parent_children * (200 / (lvl + 0.33)) +
                #         curr * (200 / (lvl + 0.33))), parentPos[1] + 5]

                try:
                    curr = currChildren[parentId]
                except KeyError:
                    currChildren[parentId] = 0

                if parent_children == 1:
                    nodePos = [parentPos[0], parentPos[1] + 5]
                    nodes[node]["pos"] = nodePos
                    pos[node] = nodePos
                    currChildren[parentId] = currChildren[parentId] + 1
                    parent_edges.append((parentId, node))
                    continue

                thisLvlWidth = float(level_width) / float(lvl * lvl)
                thisLvlStep = float(thisLvlWidth) / float(parent_children)

                # print("level_width", level_width)
                # print("lvl", lvl)
                print("node:", node)
                print("thisLvlWidth", thisLvlWidth)
                print("thisLvlStep", thisLvlStep)

                # nodePos = (parentPos[0] - thisLvlWidth / 2 +
                #            currChildren[parentId * thisLvlStep, parentPos[1] + 5])

                # nodePos = [parentPos[0] - thisLvlWidth / 2 +
                #            (currChildren[parentId]) * thisLvlStep, parentPos[1] + 5]

                nodePos = [
                    (parentPos[0] - (thisLvlStep * int(parent_children / 2)) + (currChildren[parentId]) * thisLvlStep), parentPos[1] + 5]

                nodes[node]["pos"] = nodePos
                pos[node] = nodePos
                currChildren[parentId] = currChildren[parentId] + 1
                parent_edges.append((parentId, node))

            else:
                parentPos = (parent_less_nodes, -10)
                parent_less_nodes += 100
                print("err: {} has no parent".format(node))
    # print(latencyEdges)

    '''
    latVals = [latencyEdges[l] for l in latencyEdgeLabels]
    print(latVals)
    n, bins, patches = plt.hist(latVals, 50,facecolor='green', alpha=0.75)
    plt.show()
    '''
    for p in pos:
        aux = int(pos[p][1])
        pos[p][1] = (max_level - aux)

    fig, ax = plt.subplots(figsize=(18, 8))
    fig.tight_layout()

    node_list = [n for n in pos]

    minLat = 10000000000000000
    maxLat = -1

    for latPair in latencyEdges:
        currLatVal = latencyEdges[latPair]
        minLat = min(minLat, currLatVal)
        maxLat = max(maxLat, currLatVal)

    edge_colors = [latencyEdges[l] for l in latencyEdges]
    # print()

    print(json.dumps(nodes, indent=4, sort_keys=True))
    # for node in nodes:
    #     print("{}:{}".format(node, nodes[node]))

    parent_colors = []
    # print(latencyEdges)

    for p in parent_edges:
        # print(p)
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

    # print(parent_colors)
    # print(minLat, maxLat)
    # print(edge_colors)
    # pos = nx.spring_layout(node_list, pos=pos, iterations=10000)

    with open('{}/parent_edges.txt'.format(output_folder), 'w') as f:
        for node in sorted(nodes, key=lambda x: nodes[x]["node_level"], reverse=False):
            if nodes[node]["node_level"] == 0:
                f.write("{} ".format(node))
        f.write("\n")
        for parent_edge in parent_edges:
            f.write("{} {}\n".format(parent_edge[0], parent_edge[1]))

    cmap = plt.cm.rainbow
    print(pos)
    nx.draw_networkx_nodes(G, pos, nodelist=node_list,
                           node_size=300, ax=ax, node_shape="o")
    nx.draw_networkx_labels(G, pos, nodeLabels, font_size=6, ax=ax)
    nx.draw_networkx_edges(G, pos, edgelist=parent_edges,
                           edge_color=parent_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52, width=4, ax=ax)
    nx.draw_networkx_edges(G, pos, style='dashed', edgelist=latencyEdges, width=1,
                           alpha=0.5, edge_color=edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52, ax=ax)
    nx.draw_networkx_edge_labels(
        G, pos, latencyEdgeLabels, label_pos=0.33, alpha=0.5, font_size=6, ax=ax)

    print(minLat, maxLat)

    cbaxes = fig.add_axes([0.95, 0.05, 0.01, 0.65])
    norm = mpl.colors.Normalize(vmin=minLat, vmax=maxLat)
    cb1 = mpl.colorbar.ColorbarBase(
        cbaxes, cmap=cmap, norm=norm, orientation='vertical')
    plt.savefig("{}/topology.png".format(output_folder))


#    plt.show()

def main():
    log_folder, output_folder = parse_args()
    paths = []
    for node_folder in os.listdir(log_folder):
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))

    parse_files(paths, output_folder)


if __name__ == "__main__":
    main()
