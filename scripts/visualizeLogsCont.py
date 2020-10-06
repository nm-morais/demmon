#!/usr/bin/env python3

import subprocess
import time
import cv2


plot_filepath_remote = "/home/nunomorais/git/nm-morais/deMMon"
plot_filepath_local = "/Users/nunomorais/go/src/github.com/nm-morais/deMMon/topology.png"
logs_folder="/tmp/demmon_logs"
wait = 3

while True:
  try:
    subprocess.run(["ssh", "dicluster", "ssh node16 'python3 /home/nunomorais/git/nm-morais/deMMon/scripts/visualizeLogs.py {} {}'".format(logs_folder, plot_filepath_remote)], check=True)
  except subprocess.CalledProcessError as e:
    print(e)

  try:
    subprocess.run(["scp", "dicluster:{}/topology.png".format(plot_filepath_remote), "{}".format(plot_filepath_local)], check=True)
  except subprocess.CalledProcessError as e:
    print(e)

  image = cv2.imread(plot_filepath_local)
  cv2.imshow('graph', image)
  cv2.waitKey(0)
  
