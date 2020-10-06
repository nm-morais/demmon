#!/usr/bin/env python3

import random as rand
import argparse
import os
import json


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_folder", help="the log file")
    parser.add_argument("coords_file", help="the log file")
    parser.add_argument("output_folder", help="the output path file")
    args = parser.parse_args()
    return args.log_folder, args.coords_file, args.output_folder


def parse_files(file_paths, output_folder):
    level_width = 700
    landmarks = 0
    nodes = {}
    attrs = {}
    max_level = -1
    
    parent_less_nodes = 0
    print()

    for file_path in file_paths:
        f = open(file_path, "r")
        node_name = str(file_path.split("/")[-2])
        node_id = node_name.split(":")[0][6:]
        lines = f.readlines()
        print(file_path, "lines:{}".format(len(lines)))
        latencies_added = {}
        neighbours = []
        for aux in reversed(lines):
            line = aux.strip()
            keyWord = "InView"
            if keyWord in line:
                # print(line)
                idx = line.index(keyWord) + len(keyWord) + 1
                # print(idx)
                toParse = line[idx:]
                # print(toParse)
                toParseTrimmed = toParse.strip()
                split = toParseTrimmed.split(" ")
                for i , ip in enumerate(split):
                    print(i, ip)
                    neighbour_ip = ip.split(":")[0][6:]
                    neighbours.append(neighbour_ip)
                break

        nodes[node_id]= {
            "id" : node_id,
            "neighbours" : neighbours
        }
    return nodes


def read_conf_file(file_path):
    f = open(file_path, "r")
    node_positions = {}
    for aux in f.readlines():
        line = aux.strip()
        # print(line)
        split = line.split(" ")
        node_id = split[0]
        node_x = split[1]
        node_y = split[2]
        node_positions[node_id] = [int(node_x), int(node_y)]
    return node_positions

def main():
    log_folder, coords_file ,output_folder = parse_args()
    paths = []
    for node_folder in os.listdir(log_folder):
        node_path = "{}/{}".format(log_folder, node_folder)
        for node_file in os.listdir(node_path):
            if node_file == "DemonTree.log":
                paths.append("{}/{}".format(node_path, node_file))

    node_connections = parse_files(paths, output_folder)
    node_positions = read_conf_file(coords_file)

    for node_id in node_positions :
        node_connections[node_id]["coords"] = node_positions[node_id]

    # print(node_connections)
    json_data = json.dumps(node_connections,  indent=4, sort_keys=True)
    print(json_data)
    f = open("nodeConnections.json", "w")
    print(f.write(json_data)) 
    # print(node_positions)

if __name__ == "__main__":
    main()
