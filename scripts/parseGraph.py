from typing import ForwardRef
from graph_tool.all import *
import math
import csv
import numpy as np
from tqdm import tqdm


def cartesian_dist(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)


def already_added(g, v, added_map):
    # p = (g.vp.lat[v], g.vp.long[v])
    # for k in added_map:
    #     if cartesian_dist(p, added_map[k]) < 0.001:
    #         return k
    return None


# Cada vertice tem as seguintes propriedades:
# "nid" - NodeID
# "lat" - Latitude
# "long" - Longitude
# "region" - Regiao (dentro do pais, tipo "TX"-- texas; "CA" --california)
# "continent" - Continente - EU, AS, NA, SA, OC, AF
# "country" - Pais - PT, UK, ES, HU ...
# "city - Cidade - Lisboa, Setubal, Porto, Vila Nova de Mil Fontes

# "Cade edge tem as seguintes propreiedas
# "ipSrc" - Ip source
# "ipDest" - Ip destino
# "lat" - latencia
# "var" - variancia
# Tambem podes depois filtrar por regiao, ou coordenadas geograficas
# g = GraphView(g, vfilt=lambda v: g.vp.city[v] in [
#               "Lisboa", "Madrid", "Paris"])

# Ou filtar atravez de proprieades dos edges
# g = GraphView(g, efilt=lambda e: 150 > g.ep.lat > 50)

# Depois podes iterar sobre os vertices:

# taken_latencies = []
# for i, v in enumerate(g.vertices()):
#     dist_map, pred_map = dijkstra_search(g, g.vp.lat, v)
#     print(res)
#     if i == 1:
#         break


if __name__ == "__main__":

    # faz load do graph para memoria
    g = load_graph("config/graph.xml.gz")
    # Vais buscar a maior component conexa que é o dos 8mil
    g.list_properties()
    g.set_directed(False)
    g = GraphView(g, vfilt=label_largest_component(g))
    added_map = {}
    conf_f = open("config/coords_file.txt", 'w', newline='')
    lat_f = open("config/lats_file.txt", 'w', newline='')
    conf_writer = csv.writer(conf_f, delimiter=" ")
    lats_writer = csv.writer(lat_f, delimiter=" ")
    edges_to_add = []
    for v in g.iter_vertices():
        # print(v)
        # node_already_added = already_added(g, v, added_map)
        # if node_already_added is not None:
        # print("already added:", node_already_added)
        # print(g.ep.lat)
        # g.ep.lat[new_edge] = g.ep.lat[(v, v2)]
        # print(
        #     f"Skipping node {v} with lat: {g.vp.lat[v]}, lon:{g.vp.long[v]}")
        # exit()
        # else:
        #     added_map[v] = (g.vp.lat[v], g.vp.long[v])

        for e in g.iter_out_edges(v, eprops=[g.ep.lat]):
            edges_to_add.append((e[0], e[1],  e[2] * 0.3))

    # print("adding edge list")
    # print(edges_to_add)

    g.add_edge_list(edges_to_add, eprops=[g.ep.lat])
    g = GraphView(g, efilt=lambda e: g.ep.lat[e] < 50)
    g = GraphView(g, vfilt=label_largest_component(g))
    n_nodes = 0
    for v in g.iter_vertices():
        n_nodes += 1
    print("filtering by edges")
    print("getting shortest distances...")
    rand_vertex = np.random.choice([v for v in g.iter_vertices()], 1000)
    pbar = tqdm(total=len(rand_vertex))
    for v in rand_vertex:
        dist_map = shortest_distance(
            g, source=v, target=rand_vertex, weights=g.ep.lat)
        lats_writer.writerow(['{:.2f}'.format(x) for x in dist_map])
        conf_writer.writerow([g.vp.lat[v], g.vp.long[v]])
        pbar.update(1)
    pbar.close()

    print(f"Wrote coords file to: config/coords_file.txt")
    print(f"Wrote coords file to: config/lats_file.txt")
