import csv

import sys

import numpy as np
import igraph as ig

from ipaddress import *

print('Args:', str(sys.argv))

with open(sys.argv[1]) as inet_output:
    reader = csv.reader(inet_output, delimiter='\t')
    # ignore first line
    parsed = [n for n in reader][1:]

n_nodes = int(sys.argv[2])
nodes_prop = parsed[:n_nodes]

nodes = [int(n[0]) for n in nodes_prop]
print("nodes="+str(nodes))
coords = [(int(n[1]), int(n[2])) for n in nodes_prop]
print("coords="+str(coords))

edges_prop = parsed[n_nodes:]

print(edges_prop)

edges = [(int(e[0]), int(e[1])) for e in edges_prop]
wedges = [int(e[2]) for e in edges_prop]

print(edges)
print(wedges)

g = ig.Graph()
g.add_vertices(nodes)
g.add_edges(edges)
g.es["weight"] = wedges
g.vs["name"] = nodes
g.vs["coords"] = coords

#ig.plot(g, "inet10000.png", bbox=(10000,10000))

print(g.degree())

# print(g)

sub100 = nodes[:100]
g100: ig.Graph = g.induced_subgraph(sub100)


# uncomment to generate image of graph
#ig.plot(g100, "inet100.png", bbox=(10000,10000))
#ig.plot(g100, "inet100Labeled.png", vertex_label=g100.vs["name"] ,bbox=(10000,10000))

mat100 = g100.shortest_paths(weights=g100.es["weight"], mode=ig.ALL)
print(mat100)
niceMat=np.mat(mat100)
print(niceMat)
niceMat = niceMat*0.005
mat1 = g100.shortest_paths(source=g100.vs["name"].index(0), weights=g100.es["weight"], mode=ig.ALL)
print(mat1)

# uncomment to write file
#with open('inet100Latencies_x0.005.txt', 'wb') as f:
#    for line in niceMat:
#        np.savetxt(f, line, fmt='%.2f')

def genIp(net):
    ip = next(net.hosts())
    mask = net.hostmask
    #print(str(bin(int(ip))))
    #print(str(bin(int(mask))))

    pre = net.prefixlen;

    #newIp = int(ip) ^ int(mask)
    newIp = int(ip) + pre
    return IPv4Address(newIp)



tree = [[0]]
names= [["node0_0_0"]]
nets = [[ip_network('10.10.0.0/16')]]

i=0
n_childs=3
while g100.get_edgelist():
    layer = tree[i]
    to_add = []
    subnets = []
    names_in_layer= []
    j = 0
    for n in layer:
        m = g100.vs["name"].index(n)
        #print(str(m) + " , " + str(n))
        spd = g100.shortest_paths_dijkstra(source=m, weights=g100.es["weight"])
        root: list = spd[0].copy()
        root.sort()
        childs = 0
        while root and childs < n_childs:
            c = root[0]
            root.remove(c)
            elem = g100.vs["name"][spd[0].index(c)]
            if elem not in layer and elem not in to_add:
                to_add.append(elem)
                supernets = nets[i]
                supernet = supernets[layer.index(n)]
                subs = supernet.subnets(prefixlen_diff=n_childs)
                for x in range(0, childs):
                    next(subs)
                subnets.append(next(subs))
                names_in_layer.append("node"+str(i+1)+"_"+str(j)+":"+str(childs))
                childs = childs + 1

        g100.delete_vertices(m)
        j = j +1

    i=i+1
    tree.append(to_add)
    nets.append(subnets)
    names.append(names_in_layer)
    print(tree)
    print(nets)


i=0
for layer in tree:
   print("layer="+str(i)+ ", len=" + str(len(layer)) + " " + str(layer))
   i = i+1

ips = []
i=0
for net in nets:
    layer = []
    j=0
    for n in net:
        ip = genIp(n)
        #print(ip)
        layer.append(ip)
        j=j+1
    ips.append(layer)
    i=i+1

def getPropByIdx(idx, tree, prop):
    i =0
    for layer in tree:
        j=0
        for n in layer:
            if n == idx:
                return prop[i][j]
            j = j+1
        i = i+1

def getIdxLayer(idx, tree):
    i =0
    for layer in tree:
        if idx in layer:
            return i
        i=i+1


""""
#write config file
with open("config100.txt", "w") as f:
    for i in range(0, 100):
        f.write("{layer} {ip} {name}\n".format(layer=str(getIdxLayer(i, tree)),
                                               ip=str(getPropByIdx(i, tree, ips)),
                                               name=str(getPropByIdx(i, tree, names))))
"""

""""
#write ips file 
with open("ips100.txt", "w") as f:
    for i in range(0, 100):
        f.write("{ip}\n".format(ip=str(getPropByIdx(i, tree, ips))))
"""

i=0
for layer in ips:
   print("layer="+str(i)+ ", len=" + str(len(layer)) + " " + str(layer))
   i = i+1


i=0
for layer in ips:
    s = "layer="+str(i)
    for d in layer:
        s = s + " "+ str(bin(int(d))) +" "
    print(s)
    i = i+1

ip = int(ips[0][0])
for layer in ips:
    s = "layer="+str(i)
    for d in layer:
        s = s + " "+ str(abs(ip - int(d))) +" "
    print(s)
    i = i+1



"""    
print(spd)
root: list = spd[0].copy()
root.sort()
root.remove(0)
print(root)
childs = root[:4]
print(childs)
other: list = spd[0]
print(other)
c1 = other.index(root[0])
c2 = other.index(root[1])
c3 = other.index(root[2])
c4 = other.index(root[3])

print(str(c1) + ", " + str(c2) + ", " + str(c3) + ", " + str(c4))

g100.delete_vertices([0,c1,c2,c3,c4])
print(g100.vs["name"])
"""

"""""
print(g100.vs["name"])
tree = [[0]]
g100.delete_vertices(0)
print(g100.vs["name"])
#ig.plot(g100, vertex_label=g100.vs["name"] ,bbox=(10000,10000))
tree.append([1,2,3,4])
g100.delete_vertices(range(4))
print(g100.vs["name"])
#ig.plot(g100, vertex_label=g100.vs["name"] ,bbox=(10000,10000))
tree.append(g100.vs["name"][slice(0, 4*len(tree[1]))])
g100.delete_vertices(range(4*len(tree[1])))
print(g100.vs["name"])
ig.plot(g100, vertex_label=g100.vs["name"] ,bbox=(10000,10000))
"""