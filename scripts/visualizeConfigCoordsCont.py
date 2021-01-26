#!/usr/bin/env python3

import cv2
import subprocess

plot_filepath = "/Users/nunomorais/go/src/github.com/nm-morais/demmon/topology_coords.png"
plot_folder = "/Users/nunomorais/go/src/github.com/nm-morais/demmon/"
parent_edges = "parent_edges.txt"
latency_edges = "config/bruno_100_latencies.txt"
config_file = "config/bruno_100_coords.txt"
wait = 3

# scp dicluster:/home/nunomorais/git/nm-morais/demmon/parent_edges.txt .
while True:
    try:
        subprocess.run(["scp", "dicluster:/home/nunomorais/git/nm-morais/demmon/parent_edges.txt", plot_folder],
                       check=True)
        subprocess.run(["./scripts/visualizeConfigCoords.py", config_file, parent_edges, plot_folder, latency_edges],
                       check=True)
    except subprocess.CalledProcessError as e:
        print(e)
        cv2.waitKey(0)
    image = cv2.imread(plot_filepath)
    cv2.imshow('graph', image)
    cv2.waitKey(0)
