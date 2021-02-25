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

cidr_provided = "10.10.0.0/16"
network = "test_net"


def run_cmd_with_try(cmd, env=dict(os.environ), stdout=subprocess.DEVNULL):
    print(f"Running | {cmd} | LOCAL")
    cp = subprocess.run(cmd, shell=True, stdout=stdout, env=env)
    if cp.stderr is not None:
        raise Exception(cp.stderr)


def exec_cmd_with_output(cmd):
    (status, out) = subprocess.getstatusoutput(cmd)
    if status != 0:
        print(out)
        exit(1)
    return out


def get_ips():

    ips = [str(ip) for ip in IPNetwork(cidr_provided)]
    # Ignore first two IPs since they normally are the NetAddr and the Gateway, and ignore last one since normally it's the
    # broadcast IP
    ips = ips[2:-1]
    entrypoints_ips = set()
    rm_anchor_cmd = f"docker rm -f anchor || true"
    run_cmd_with_try(rm_anchor_cmd)
    print(f"Setting up anchor")
    anchor_cmd = f"docker run -d --name=anchor --network={network} alpine sleep 1d"
    run_cmd_with_try(anchor_cmd)

    """
    Output is like:
    "lb-swarm-network": {
        "Name": "swarm-network-endpoint",
        "EndpointID": "ab543cead9c04275a95df7632165198601de77c183945f2a6ab82ed77a68fdd3",
        "MacAddress": "02:42:c0:a8:a0:03",
        "IPv4Address": "192.168.160.3/20",
        "IPv6Address": ""
    }
    so we split at max once thus giving us only the value and not the key
    """

    get_entrypoint_cmd = f"docker network inspect {network} | grep 'lb-{network}' -A 6"
    output = exec_cmd_with_output(get_entrypoint_cmd).strip().split(" ", 1)[1]

    entrypoint_json = json.loads(output)

    entrypoints_ips.add(entrypoint_json["IPv4Address"].split("/")[0])
    get_anchor_cmd = f"docker network inspect {network} | grep 'anchor' -A 5 -B 1"
    output = exec_cmd_with_output(get_anchor_cmd).strip().split(" ", 1)[1]
    if output[-1] == ",":
        output = output[:-1]

    anchor_json = json.loads(output)
    entrypoints_ips.add(anchor_json["IPv4Address"].split("/")[0])

    print(f"entrypoints: {entrypoints_ips}")
    return reversed(ips)


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

    ips = get_ips()

    for i, v in enumerate(added_map):
        dist_map = shortest_distance(
            g, source=v, target=[_v for _v in added_map], weights=g.ep.lat)
        lats_writer.writerow(['{:.2f}'.format(x) for x in dist_map])
        conf_writer.writerow(
            [f"node{i}", f"{ips[i]}", g.vp.lat[v], g.vp.long[v]])
