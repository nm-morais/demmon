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
    parser.add_argument("config_file", help="the log file")
    parser.add_argument("latencies_file", help="the latencies file")
    args = parser.parse_args()
    return args

def read_conf_file(file_path):
    f = open(file_path, "r")
    for line in reversed(f.readlines()):
        print(line)

def main():
    args = vars(parse_args())
    print(args)
    read_conf_file(args["config_file"])

if __name__ == "__main__":
    main()



