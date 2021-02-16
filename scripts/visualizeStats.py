#!/usr/bin/env python3


from multiprocessing import Process, Manager
import json
import threading
import networkx as nx
import pandas as pd
from dateutil.parser import parse
import matplotlib
import time
import numpy as np
import os
import argparse

latency_collection_tag = "<latency_collection>"
inview_tag = "<inView>"


def get_file_list(log_folder):
    paths = []
    node_folders = os.listdir(log_folder)
    for node_folder in node_folders:
        if not node_folder.startswith("10.10"):
            print(f"skipping folder: {node_folder}")
            continue
        print(f"parsing folder: {node_folder}")
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))
    return paths, len(paths)


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


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",  metavar='config_file',
                        type=str, help="the log file")

    parser.add_argument("--latencies_file",  metavar='latencies_file',
                        type=str,  help="the latencies file")

    parser.add_argument("--logs_folder",   metavar='logs_folder',
                        type=str,   help="the folder where logs are contained")

    parser.add_argument("--output_path",  metavar='output_path',
                        type=str,  help="the output file")

    parser.add_argument("--coords_file",  metavar='coords_file',
                        type=str,  help="the coords file")

    parser.add_argument("--parent_edges_file",  metavar='parent_edges_file',
                        type=str,  help="the name of the file containing the parenthood links")

    args = parser.parse_args()
    return args


def extractLatencyTotal(line):

    line_cut = line[line.index(
                    latency_collection_tag) + len(latency_collection_tag) + 1:len(line) - 2]
    latency_total = 0
    n_nodes = 0

    for segment in line_cut.split(";"):
        segment_cut = segment.split(":")
        if len(segment_cut) == 2:
            ip = segment_cut[0]
            latency = int(segment_cut[1]) / 2
            latency_total += int(latency)
            n_nodes += 1
            # print(f"ip: {ip}, latency: {latency}")

    return latency_total, n_nodes


def extractInView(line):
    # print("line", line)
    line_cut = line[line.index(inview_tag) + len(inview_tag) + 1:len(line) - 1]
    line_cut = line_cut.replace("\\", "")
    return json.loads(line_cut)


def parse_file(file, node_ip, node_infos):
    f = open(file, "r")

    node_measurements = {
        "latency_avg": [],
        "latency_totals": [],
        "timestamp": [],
        "ip": [],
        "timestamp_dt": [],
        "latencies": [],
        "degree": [],
        "landmark": False,
        "level": -1,
        "parent": "",
        "pos": [0, 0],
    }

    added = 0
    for aux in f.readlines():
        line = aux.strip()
        if latency_collection_tag in line:
            latency_total, n_nodes_in_measurement = extractLatencyTotal(
                line)

            if n_nodes_in_measurement == 0:
                continue
            latency_avg = latency_total / n_nodes_in_measurement

            timeStr = "time="
            levelStr = " level="

            ts = line[line.find(timeStr) + len(timeStr) + 1:]
            ts = ts[:line.find(levelStr) - len(levelStr) - 1]
            ts_parsed = parse(ts)
            added += 1
            node_measurements["latency_avg"].append(latency_avg)
            node_measurements["latency_totals"].append(latency_total)
            node_measurements["timestamp"].append(ts_parsed)
            node_measurements["timestamp_dt"].append(pd.to_datetime(ts_parsed))
            node_measurements["degree"].append(n_nodes_in_measurement)
            node_measurements["ip"].append(node_ip)

        if "I am landmark" in line:
            node_measurements["landmark"] = True
            node_measurements["level"] = 0

        # if "My parent changed" in line:
        #     split = line.split(" ")

        #     split_line = line.split(" ")
        #     ip_port = str(split_line[11])
        #     ip = str(ip_port.split(":")[0])[6:]

        #     print(f"parent_name: {ip}")
        #     node_measurements["parent"] = ip

        # if "Latency:" in line and "[NodeWatcher]" in line:
        #     if "Lowest Latency Peer" in line:
        #         continue
        #     split = line.split(" ")
        #     ip_port = str(split[6])[:-1]
        #     ip = str(ip_port.split(":")[0])[6:]
        #     for i, j in enumerate(split):
        #         print(i, j)
        #     try:
        #         latStr = split[10]
        #         latStr2 = latStr[:-1]
        #         node_measurements["latencies"].append(
        #             (node_ip, ip, (int(latStr2) / 1000000) / 2))
        #     except Exception as e:
        #         print(f"got exception {e} on line {line}")

        if inview_tag in line:
            inView = extractInView(line)
            # print(inView)
            try:
                p = inView["parent"]
                # print("assigning parent", p)
                node_measurements["parent"] = str(p["Name"].split(":")[0])[6:]
                node_measurements["parentLat"] = int(p["Latency"])
            except Exception as e:
                # print(e)
                pass

    if added == 0:
        # print(
        #     f"warn: node {node_ip} has no active view latencies")
        return

    node_infos[node_ip] = node_measurements


def getNodeLevel(node_infos, nodeID):
    # print("Finding nodeID", nodeID)
    try:
        parent = node_infos[nodeID]["parent"]
        # print(f"finding parent: {parent}")
        if parent == "":
            return 1
        return 1 + getNodeLevel(node_infos, parent)
    except KeyError as e:
        return 0


def plotTree(node_infos, max_level, output_folder):
    # print("MAX_LEVEL: ", max_level)
    level_width = 1000
    landmarks = 0
    parent_less_nodes = 0
    children_counter = {}
    parent_edges = []
    # latencyEdges = {}
    latencyEdgeLabels = {}
    nodeLabels = {}
    minLat = 0
    maxLat = 0
    pos = {}
    children = {}
    node_level_steps = {}
    levels = {}
    parentPos = {}

    for nodeID in node_infos:
        children[nodeID] = 0

    for nodeID in node_infos:
        if node_infos[nodeID]["landmark"]:
            xPos = (landmarks + 1) * level_width * 2
            print("landmark: {}".format(nodeID))
            # print("landmark xpos: {}".format(xPos))
            landmarks += 1
            parentPos[nodeID] = [xPos, 0]
            node_level = getNodeLevel(node_infos, nodeID)
            levels[nodeID] = node_level
            continue
        else:
            try:
                parent = node_infos[nodeID]["parent"]
                children[parent] += 1
                node_level = getNodeLevel(node_infos, nodeID)
                levels[nodeID] = node_level
                # print(f"Assigned level {node_level} to node {nodeID}")
            except KeyError:
                levels[nodeID] = -1
                parentPos[nodeID] = [parent_less_nodes, -3]
                parent_less_nodes += 100
                continue

    # print(levels)
    for nodeID in levels:
        max_level = max(max_level, levels[nodeID])
        # print(levels[nodeID])
    # print(max_level)
    latencyEdges = {}

    for nodeID in sorted(node_infos, key=lambda x: levels[x], reverse=False):
        landmark = node_infos[nodeID]["landmark"]
        print(nodeID)
        if nodeID.startswith("."):
            print(f"skipping: {nodeID}")
            continue
        nodeLabels[nodeID] = nodeID
        if levels[nodeID] == -1:
            parent_less_nodes += 100
            pos[nodeID] = (parent_less_nodes, -2)
            parent_less_nodes += 100
            print("err: {} has no parent".format(nodeID))
            continue

        if landmark is True:
            pos[nodeID] = (parentPos[nodeID][0], parentPos[nodeID][1])
            # print("assigning node_level_step")
            node_level_steps[nodeID] = level_width
        else:
            parentId = node_infos[nodeID]["parent"]
            # print("parentID: ", parentId)
            # print("node_infos[nodeID]", node_infos[nodeID])
            try:
                parent = node_infos[parentId]
            except KeyError:
                parentPos = (parent_less_nodes, -2)
                parent_less_nodes += 100
                print(
                    f"err: {nodeID} has parent but parent: {parentId} not in node list")
                continue

            nodeParentPos = parentPos[parentId]
            parent_children = children[parentId]
            curr_children = 0
            try:
                curr_children = children_counter[parentId]
                children_counter[parentId] += 1
            except Exception as e:
                children_counter[parentId] = 1
            # print(node_infos[parentId])

            thisLvlWidth = node_level_steps[parentId]
            thisLvlStep = float(thisLvlWidth) / \
                float(max(parent_children - 1, 1))
            node_level_steps[nodeID] = thisLvlStep

            nodePos = ()
            if parent_children == 1:
                nodePos = [nodeParentPos[0], nodeParentPos[1] - max_level]
            else:
                nodePos = [(nodeParentPos[0] - (thisLvlWidth / 2)) +
                           (curr_children * thisLvlStep), nodeParentPos[1] - max_level]
            parentPos[nodeID] = nodePos
            pos[nodeID] = nodePos
            parent_edges.append((nodeID, parentId))
            latencyEdges[(nodeID, parentId)] = node_infos[nodeID]["parentLat"]

        # for latencyPair in node_infos[nodeID]["latencies"]:
        #     print(
        #         f"Adding latency edge between {latencyPair[0]} and {latencyPair[1]}")
        #     minLat = min(int(latencyPair[2]), minLat)
        #     maxLat = max(int(latencyPair[2]), maxLat)
        #     latencyEdges[(latencyPair[0], latencyPair[1])
        #                  ] = int(latencyPair[2])

    # edge_colors = [latencyEdges[l] for l in latencyEdges]

    parent_colors = []
    for p in parent_edges:
        try:
            latencyEdgeLabels[p] = latencyEdges[p]
            parent_colors.append(latencyEdges[p])
        except KeyError:
            try:
                latencyEdgeLabels[(p[1], p[0])] = latencyEdges[(p[1], p[0])]
                parent_colors.append(latencyEdges[(p[1], p[0])])
            except KeyError:
                print(f"Missing latency edge between {p[0]} and {p[1]}")
                latencyEdgeLabels[(p[1], p[0])] = "missing"
                parent_colors.append(1000000000)

    # latVals = [latencyEdges[l] for l in latencyEdgeLabels]
    # n, bins, patches = plt.hist(latVals, 50, facecolor='green', alpha=0.75)
    # plt.savefig("{}/histogram.svg".format(output_folder), dpi=1200)

    minY = 100000
    maxY = -100000

    for p in pos:
        minY = min(pos[p][1], minY)
        maxY = max(pos[p][1], maxY)
    # print(minY, maxY)
    import matplotlib.pyplot as plt
    print(len(pos))
    G = nx.Graph()
    cmap = plt.cm.rainbow
    fig, ax = plt.subplots(figsize=(10, 10))
    plt.ylim(minY - 2, maxY + 2)
    fig.tight_layout()
    cbaxes = fig.add_axes([0.89, 0.6, 0.005, 0.33])

    norm = matplotlib.colors.Normalize(vmin=minLat, vmax=maxLat)
    matplotlib.colorbar.ColorbarBase(
        cbaxes, cmap=cmap, norm=norm, orientation='vertical')
    nx.draw_networkx_nodes(G, pos, nodelist=nodeLabels,
                           node_size=50, ax=ax, node_shape="o")
    nx.draw_networkx_labels(G, pos, nodeLabels, font_size=2, ax=ax)
    nx.draw_networkx_edges(G, pos, edgelist=parent_edges,
                           edge_color=parent_colors, edge_cmap=cmap, width=1, ax=ax)
    # nx.draw_networkx_edges(G, pos, style='dashed', edgelist=latencyEdges, width=1,
    #                        alpha=0.5, edge_color=edge_colors, edge_cmap=cmap, edge_vmin=25.6, edge_vmax=459.52, ax=ax)
    # nx.draw_networkx_edge_labels(
    #     G, pos, latencyEdgeLabels, label_pos=0.33, alpha=0.5, font_size=6, ax=ax)

    print(f"saving topology to: {output_folder}")
    plt.savefig("{}topology.svg".format(output_folder))
    # print("parent_edges:\t", parent_edges)
    return parent_edges, landmarks


def parse_file_list(file_list):
    manager = Manager()
    d = manager.dict()
    processes = []
    for file in file_list:
        node_name = str(file.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]
        p = Process(target=parse_file, args=(
            file, node_ip, d))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    return d


def plotConfigMapAndConnections(node_positions, node_ids, parent_edges, landmarks, latencies, output_folder):
    pos = node_positions
    print(pos)
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.autoscale(enable=True, axis='both', tight=True)
    fig.tight_layout()
    G = nx.DiGraph()
    regNodes = []
    labels = {}
    pos_higher = {}

    for k in node_positions:
        v = node_positions[k]
        if (v[1] > 0):
            pos_higher[k] = (v[0] + 150, v[1] + 150)
        else:
            pos_higher[k] = (v[0] - 150, v[1] - 150)

    for node in node_positions:
        if node not in landmarks:
            regNodes.append(node)

    for node in node_positions:
        labels[node] = node

    # print("edgelist:\t", edgelist)
    nx.draw_networkx_nodes(G, pos, nodelist=regNodes,
                           node_color='b', node_size=35, alpha=1)
    nx.draw_networkx_nodes(G, pos, nodelist=landmarks,
                           node_color='r', node_size=35, alpha=1)
    nx.draw_networkx_labels(G, pos_higher, labels, font_size=7,
                            font_family="sans-serif", font_color="black")
    nx.draw_networkx_edges(G, pos, arrowsize=12, style='dashed', arrowstyle="->", edgelist=parent_edges[0], width=1,
                           alpha=1)
    plt.axis("off")
    print(f"saving config with coords to: {output_folder}")
    plt.savefig(f"{output_folder}topology_coords.svg", dpi=1200)


def plot_avg_latency_all_nodes_over_time(df, output_path):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    resampled = df[["latency_avg", "latency_avg_global"]
                   ].resample('5s').mean()
    resampled.drop(resampled.tail(1).index,
                   inplace=True)
    resampled.plot(ax=ax)

    ax.set(xlabel='time (s)', ylabel='latency (ms)',
           title='Average latency over time in active view')
    ax.grid()
    print(f"saving average latency over time in active view to: {output_path}")
    fig.savefig(f"{output_path}latencies_over_time.svg", dpi=1200)


def plot_avg_degree_all_nodes_over_time(df, output_path):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    resampled = df[["degree"]].resample('5s').mean()
    resampled.drop(resampled.tail(1).index,
                   inplace=True)
    resampled.plot(ax=ax)
    ax.set(xlabel='time (s)', ylabel='degree',
           title='Average degree of nodes over time')
    ax.grid()
    print(f"saving average degree of nodes over time to: {output_path}")
    fig.savefig(f"{output_path}degree_over_time.svg", dpi=1200)


def plot_degree_hist_last_sample(node_infos, output_path):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    node_degrees = []
    max_degree = -10
    for info in node_infos:
        # print(node_infos[info].keys())
        # print(node_infos[info]["degree"][-1])
        # print(info, node_infos[info]["degree"][-1])
        max_degree = max(max_degree, node_infos[info]["degree"][-1])
        node_degrees.append(node_infos[info]["degree"][-1])
        # {"degree":, "ip": node_infos[info]["ip"]})

    # density=False would make counts
    bins = range(0, max_degree)
    counts, bins, patches = plt.hist(node_degrees, bins=bins)
    maxCount = 0
    for curr in counts:
        maxCount = max(maxCount, curr)
    ax.grid()
    yTticks = range(0, int(maxCount) + 3)
    ax.set_xticks(bins)
    ax.set_yticks(yTticks)
    ax.set(xlabel='Degree of nodes', ylabel='number of nodes',
           title='Histogram of degree of nodes in last sample')
    print(f"saving histogram of degree of nodes in last sample: {output_path}")
    fig.savefig(f"{output_path}hist_degree.svg", dpi=1200)


def read_coords_file(file_path):
    f = open(file_path, "r")
    node_positions = {}
    node_ids = []
    for aux in f.readlines():
        line = aux.strip()
        split = line.split(" ")
        node_id = split[0]
        node_x = split[1]
        node_y = split[2]
        node_positions[node_id] = (float(node_x), float(node_y))
        node_ids.append(node_id)
    return node_positions, node_ids


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


def main():

    args = parse_args()
    print("args: ", args)
    node_ids = read_conf_file(args.config_file)
    latencies = read_latencies_file(args.latencies_file)
    file_list, n_nodes = get_file_list(args.logs_folder)
    print(f"Processing {n_nodes} nodes")
    node_infos = parse_file_list(file_list=file_list)
    landmarks = []
    for node in sorted(node_infos, key=lambda x: node_infos[x]["level"], reverse=False):
        if node_infos[node]["level"] == 0:
            landmarks.append(node)
    print(f"landmarks: {landmarks}")
    system_lat_avg = 0
    for node_lats in latencies[:n_nodes]:
        node_lat_avg = 0
        for lat in node_lats[:n_nodes]:
            node_lat_avg += lat / n_nodes
        # print(node_lat_avg)
        system_lat_avg += node_lat_avg / n_nodes

    pd_data = {
        "ip": [],
        "latency_avg": [],
        "timestamp": [],
        "latency_avg_global": [],
        "degree": [],
    }
    max_level = -2
    for k in node_infos:
        max_level = max(max_level, node_infos[k]["level"])
        pd_data["degree"] += node_infos[k]["degree"]
        pd_data["ip"] += node_infos[k]["ip"]
        pd_data["latency_avg"] += node_infos[k]["latency_avg"]
        pd_data["timestamp"] += node_infos[k]["timestamp_dt"]
        pd_data["latency_avg_global"] += [system_lat_avg] * \
            len(node_infos[k]["timestamp_dt"])

    df = pd.DataFrame(pd_data)
    df.index = df["timestamp"]
    # print(df)
    print("system_lat_avg:", system_lat_avg)
    plot_degree_hist_last_sample(
        node_infos=node_infos, output_path=args.output_path)
    plot_avg_latency_all_nodes_over_time(
        df=df, output_path=args.output_path)
    plot_avg_degree_all_nodes_over_time(
        df=df, output_path=args.output_path)
    parent_edges = plotTree(node_infos, max_level, args.output_path)

    node_positions, _ = read_coords_file(args.coords_file)
    # plotConfigMapAndConnections(node_positions, node_ids, parent_edges,
    #                             landmarks, latencies, args.output_path)


if __name__ == "__main__":
    main()
