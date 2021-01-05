#!/usr/bin/env python3

import cv2
import sys
import subprocess

plot_filepath_remote = "/home/nunomorais/git/nm-morais/demmon"
plot_filepath_local = "/Users/nunomorais/go/src/github.com/nm-morais/demmon/topology.png"
logs_folder = "/tmp/demmon_logs"
wait = 3


arguments = len(sys.argv) - 1
if arguments == 0:
    print("Insufficient argumments, requires: <node-addr>")
    sys.exit(1)

nodename = sys.argv[1]

while True:
    try:
        subprocess.run(["ssh", "dicluster",
                        "ssh {} 'python3 /home/nunomorais/git/nm-morais/demmon/scripts/visualizeLogs.py {} {}'".format(nodename, logs_folder, plot_filepath_remote)], check=True)
    except subprocess.CalledProcessError as e:
        print(e)

    try:
        subprocess.run(
            ["scp", "dicluster:{}/topology.png".format(
                plot_filepath_remote), "{}".format(plot_filepath_local)],
            check=True)
    except subprocess.CalledProcessError as e:
        print(e)

    image = cv2.imread(plot_filepath_local)
    cv2.imshow('graph', image)
    cv2.waitKey(0)
