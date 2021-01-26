#!/usr/bin/env python3

import pandas as pd
from dateutil.parser import parse
import time
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
import argparse

latency_collection_tag = "<latency_collection>"


def get_file_list(log_folder):
    paths = []
    node_folders = os.listdir(log_folder)
    for node_folder in node_folders:
        print(node_folder)
        if not node_folder.startswith("10.10"):
            continue
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))
    return paths, len(node_folders)


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


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",  metavar='config_file',
                        type=str, help="the log file")

    parser.add_argument("--latencies_file",  metavar='latencies_file',
                        type=str,  help="the latencies file")

    parser.add_argument(
        "--logs_folder",   metavar='logs_folder',
        type=str,   help="the folder where logs are contained")

    parser.add_argument("--output_path",  metavar='output_path',
                        type=str,  help="the output file")

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


def parse_file_list(file_list, n_nodes):

    node_infos = {}
    for file in file_list:
        node_name = str(file.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]
        node_infos[node_ip] = {}

        f = open(file, "r")
        node_measurements = []
        for idx, aux in enumerate(f.readlines()):
            line = aux.strip()
            if latency_collection_tag in line:
                latency_total, n_nodes_in_measurement = extractLatencyTotal(
                    line)
                if n_nodes_in_measurement == 0:
                    print(
                        f"warn: node {node_ip} has no active view latencies")
                    continue
                latency_avg = latency_total / n_nodes_in_measurement

                timeStr = "time="
                levelStr = " level="

                ts = line[line.find(timeStr) + len(timeStr) + 1:]
                ts = ts[:line.find(levelStr) - len(levelStr) - 1]
                # print(ts)

                node_measurements.append({
                    "latency_avg": latency_avg,
                    "latency_totals": latency_total,
                    "timeStamp": parse(ts),
                })
        node_infos[node_ip]["measurements"] = node_measurements

    return node_infos


def plot_avg_latency_all_nodes_over_time(node_infos, output_path, system_lat_avg, n_nodes):

    fig, ax = plt.subplots()

    df1 = pd.DataFrame({"timestamp": [], "latency_avgs": []})
    df1.set_index('timestamp', inplace=True)
    for node_ip in node_infos:
        # df = pd.DataFrame(node_infos[node_ip]["measurements"])
        # df['latency_rolling_avg'] = df.iloc[:, 1].rolling(window=3).mean()
        # print(df.head())
        latency_averages = [measurement["latency_avg"]
                            for measurement in node_infos[node_ip]["measurements"]]
        timestamps = [measurement["timeStamp"]
                      for measurement in node_infos[node_ip]["measurements"]]
        df2 = pd.DataFrame(
            {"timestamp": timestamps, "latency_avgs": latency_averages})
        df2.set_index('timestamp', inplace=True)

        # print(df2)
        ax.plot(timestamps, latency_averages)

    print(df1.head())

    # print(node_infos)
    # ax.plot(t, len(t) * [system_lat_avg])

    ax.set(xlabel='time (s)', ylabel='latency (ms)',
           title='Average latency over time in active view')
    ax.grid()
    print(f"saving fig to: {output_path}")
    fig.savefig(output_path)


def main():
    args = parse_args()
    print(args)
    latencies = read_latencies_file(args.latencies_file)
    file_list, n_nodes = get_file_list(args.logs_folder)
    system_lat_avg = 0

    for node_lats in latencies[:n_nodes]:
        node_lat_avg = 0
        for lat in node_lats[:n_nodes]:
            node_lat_avg += lat / n_nodes
        # print(node_lat_avg)
        system_lat_avg += node_lat_avg / n_nodes

    print("system_lat_avg:", system_lat_avg)

    node_infos = parse_file_list(
        file_list=file_list, n_nodes=n_nodes)
    # print(node_infos)
    plot_avg_latency_all_nodes_over_time(
        node_infos=node_infos, output_path=args.output_path, system_lat_avg=system_lat_avg, n_nodes=n_nodes)


if __name__ == "__main__":
    main()
