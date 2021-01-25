#!/usr/bin/env python3
import os
import argparse


def get_file_list(log_folder):
    paths = []
    for node_folder in os.listdir(log_folder):
        print(node_folder)
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "all.log":
                paths.append("{}/{}".format(node_path, node_file))
    return paths


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


def parse_file_list(file_list):

    line_filter = "<latency_collection>"

    for file in file_list:
        node_name = str(file.split("/")[-2])
        node_ip = node_name.split(":")[0][6:]

        f = open(file, "r")
        for idx, aux in enumerate(f.readlines()):
            line = aux.strip()

            if line_filter not in line:
                continue

            line_cut = line[line.index(
                line_filter) + len(line_filter) + 1:len(line) - 2]

            for segment in line_cut.split(";"):
                print(segment)
                segment_cut = segment.split(":")
                if len(segment_cut) == 2:
                    ip = segment_cut[0]
                    latency = segment_cut[1]
                    print(f"ip: {ip}, latency: {latency}")

            if idx == 10:
                break


def main():
    args = parse_args()
    print("hello")
    print(args)
    node_ids = read_conf_file(args.config_file)
    latencies = read_latencies_file(args.latencies_file)
    file_list = get_file_list(args.logs_folder)

    # print(node_ids)
    # print(latencies)
    print(file_list)
    print(args.output_path)

    parse_file_list(file_list=file_list)


if __name__ == "__main__":
    main()
