#!/usr/bin/env python3
import argparse
from graph_tool.all import *


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("map_file", help="the map file")
    args = parser.parse_args()
    return args.map_file


def main():
    coordsTaken = {}
    map_file = parse_args()
    print(map_file)
    g = load_graph("mapComplete/graph.xml.gz")
    g2 = Graph()
    g.list_properties()

    for v in g.vertices():
        coords = g.vp.coord[v]
        try:
            c = coordsTaken[(coords[0], coords[1])]
        except KeyError:
            coordsTaken[(coords[0], coords[1])] = g2.add_vertex()

    # print(coordsTaken)

    for v in g.vertices():
        # print(v)
        coords = g.vp.coord[v]
        try:
            # print((coords[0],coords[1]))
            c = coordsTaken[(coords[0], coords[1])]
            for e in v.out_edges():
                target_coords = g.vp.coord[e.target()]
                try:
                    t = coordsTaken[(target_coords[0], target_coords[1])]
                    g2.add_edge(c, t)
                except KeyError:
                    continue
        except KeyError:
            continue

    # for v in g2.vertices():
    #     print(v)
    # graph_draw(g2, output="composed-filter.svg")
    print(g)
    print(g2)


if __name__ == "__main__":
    main()
