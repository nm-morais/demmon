#!/usr/bin/env python3

import subprocess
import time
import cv2

plot_filepath = "/Users/nunomorais/go/src/github.com/nm-morais/deMMon/topology.png"
plot_folder = "/Users/nunomorais/go/src/github.com/nm-morais/deMMon/"
logs_folder="/tmp/demmon_logs"
wait = 3

while True:
  try:
    subprocess.run(["./scripts/visualizeLogs.py" ,logs_folder ,plot_folder], check=True)
  except subprocess.CalledProcessError as e:
    print(e)

  image = cv2.imread(plot_filepath)
  cv2.imshow('graph', image)
  cv2.waitKey(0)
