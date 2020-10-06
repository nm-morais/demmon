#!/usr/bin/env python3

import subprocess
import time
import cv2

plot_filepath = "/Users/nunomorais/go/src/github.com/nm-morais/deMMon/topology_coords.png"
plot_folder = "/Users/nunomorais/go/src/github.com/nm-morais/deMMon/"
parent_edges="parent_edges.txt"
latency_edges="config/inet100Latencies_x0.04.txt"
config_file="config/inet50_coords.txt"
wait = 3
# scp dicluster:/home/nunomorais/git/nm-morais/deMMon/parent_edges.txt .
while True:
  try:
    subprocess.run(["scp" ,"dicluster:/home/nunomorais/git/nm-morais/deMMon/parent_edges.txt", plot_folder], check=True)
    subprocess.run(["./scripts/visualizeConfigCoords.py", config_file, parent_edges, plot_folder, latency_edges], check=True)
  except subprocess.CalledProcessError as e:
    print(e)
    cv2.waitKey(0)
  image = cv2.imread(plot_filepath)
  cv2.imshow('graph', image)
  cv2.waitKey(0)
