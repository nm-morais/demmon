from typing import ForwardRef
from graph_tool.all import *
import math
import csv


def cartesian_dist(p0, p1):
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2)


def can_add(g, v, added):
    p = (g.vp.lat[v], g.vp.long[v])
    for a in added:
        if cartesian_dist(p, a) < 0.01:
            return False
    return True


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

    p1 = (2, 4)
    p2 = (1, 2)

    # faz load do graph para memoria
    g = load_graph("config/graph.xml.gz")
    # Vais buscar a maior component conexa que é o dos 8mil
    g = GraphView(g, vfilt=label_largest_component(g))

    added_map = {}
    added = []
    conf_f = open("config.txt", 'w', newline='')
    lat_f = open("lats.txt", 'w', newline='')
    conf_writer = csv.writer(conf_f, delimiter=" ")
    lats_writer = csv.writer(lat_f, delimiter=" ")

    for v in g.vertices():
        if not can_add(g, v, added):
            print(
                f"Skipping node {v} with lat: {g.vp.lat[v]}, lon:{g.vp.long[v]}")
            continue
        added.append((g.vp.lat[v], g.vp.long[v]))
        added_map[v] = True

    for i, v in enumerate(added_map):
        dist_map = shortest_distance(
            g, source=v, target=[_v for _v in added_map], weights=g.ep.lat)
        lats_writer.writerow(['{:.2f}'.format(x) for x in dist_map])
        conf_writer.writerow([f"node{i}", g.vp.lat[v], g.vp.long[v]])
