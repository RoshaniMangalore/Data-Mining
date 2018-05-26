import sys
import math
import heapq
import copy
import numpy as np

sampleFile=sys.argv[1]
dataFile=sys.argv[2]
number_of_clusters = int(sys.argv[3])
nrep = int(sys.argv[4])
p = float(sys.argv[5])
outputFile=sys.argv[6]

f = open(sampleFile)
data = f.readlines()
input = []
for line in data:
    if line.strip() != '':
        line = line.split(',')
        line = [i.strip() for i in line]
        line[0] = float(line[0])
        line[1] = float(line[1])
        input.append(line)
inputdict = {}
for i in range(len(input)):
    inputdict[i] = [input[i], [i]]
n = len(inputdict)

def ClusterDist(originalinputdict, vectora, vectorb):
    cluster1 = vectora[1]
    l1 = len(cluster1)
    cluster2 = vectorb[1]
    l2 = len(cluster2)
    mindist = float('inf')
    for i in range(l1):
        for j in range(l2):
            if vectora[1][i] in originalinputdict and vectorb[1][j] in originalinputdict:
                point1 = originalinputdict[vectora[1][i]][0]
                point2 = originalinputdict[vectorb[1][j]][0]
                x1 = point1[0]
                y1 = point1[1]
                x2 = point2[0]
                y2 = point2[1]
                diffx = x2 - x1
                diffy = y2 - y1
                Sum = (diffx ** 2) + (diffy ** 2)
                distance = math.sqrt(Sum)
                if distance < mindist:
                    mindist = distance
    return mindist

def findcentroid(vectora, vectorb):
    x1 = vectora[0][0]
    x2 = vectorb[0][0]
    y1 = vectora[0][1]
    y2 = vectorb[0][1]
    cluster1 = vectora[1]
    l1 = len(cluster1)
    cluster2 = vectorb[1]
    l2 = len(cluster2)
    cluster1.extend(cluster2)
    length = len(cluster1)
    avgx = ((l1 * x1) + (l2 * x2)) / length
    avgy = ((l1 * y1) + (l2 * y2)) / length
    return [[avgx, avgy], cluster1]

def dist(point1, point2):
    x1 = point1[0]
    y1 = point1[1]
    x2 = point2[0]
    y2 = point2[1]
    diffx = x2 - x1
    diffy = y2 - y1
    Sum = (diffx ** 2) + (diffy ** 2)
    distance = math.sqrt(Sum)
    return distance

def assign_clust(point, replist):
    adist = float('inf')
    for i in range(len(replist)):
        minval = float('inf')
        for j in range(len(replist[i])):
            edist = dist(point, replist[i][j])
            if edist < minval:
                minval = edist
        if minval < adist:
            adist = minval
            clustnum = i
    return clustnum

originalinputdict = copy.deepcopy(inputdict)
while (len(inputdict) > number_of_clusters):
    heaplist = []
    distancedict = {}
    listt = []
    for a in range(len(originalinputdict) - 1):
        for b in range(a + 1, len(originalinputdict)):
            if a in inputdict and b in inputdict:
                vec1 = inputdict[a]
                vec2 = inputdict[b]
                ed = ClusterDist(originalinputdict, vec1, vec2)
                if ed not in distancedict.keys():
                    distancedict[ed] = (a, b)
                    heapq.heappush(heaplist, ed)

    smallestdistance = heapq.heappop(heaplist)

    coord = distancedict[smallestdistance]
    value1 = inputdict[coord[0]]
    value2 = inputdict[coord[1]]
    newcoord = findcentroid(value1, value2)
    del (inputdict[coord[0]])
    del (inputdict[coord[1]])
    inputdict[n] = newcoord
    originalinputdict[n] = newcoord
    n = n + 1
keys = []
for i in range(len(inputdict)):
    keys.append(i)
values = inputdict.values()
inputdict = dict(zip(keys, values))
for k, v in inputdict.items():
    for i in range(len(v[1])):
        if v[1][i] in originalinputdict.keys():
            v[1][i] = originalinputdict[v[1][i]][0]
centroids = []
for i in range(0, number_of_clusters):
    centroid = inputdict.values()[i][0]
    centroids.append(centroid)
n_reppoints = []
for i in range(number_of_clusters):
    reppoints = []
    len_clust = len(inputdict.values()[i][1])
    clust = inputdict.values()[i][1]
    maxdist = 0
    target1 = min(x[0] for x in clust)
    matches = [x for x in clust if (x[0] == target1)]
    if len(matches) >= 2:
        target2 = min(x[1] for x in matches)
        point1 = [x for x in matches if (x[1] == target2)]
    else:
        point1 = matches
        reppoints.append(point1[0])
        clust.remove(point1[0])
    for r in range(len_clust - 1):
        x1 = point1[0][0]
        y1 = point1[0][1]
        x2 = clust[r][0]
        y2 = clust[r][1]
        diffx = x2 - x1
        diffy = y2 - y1
        Sum = (diffx ** 2) + (diffy ** 2)
        distance = math.sqrt(Sum)
        if distance > maxdist:
            maxdist = distance
            point2 = clust[r]
    reppoints.append(point2)
    clust.remove(point2)
    while len(reppoints) < nrep:
        mindict = []
        for m in range(len(clust)):
            minlist = []
            for n in range(len(reppoints)):
                distance = dist(clust[m], reppoints[n])
                minlist.append(distance)
            mindict.append((min(minlist), clust[m]))
        distantpoint = sorted(mindict, key=lambda l: l[0], reverse=True)
        reppoints.append(distantpoint[0][1])
        clust.remove(distantpoint[0][1])
    print reppoints
    n_reppoints.append(reppoints)
moved_reppoints = copy.deepcopy(n_reppoints)
for i in range(number_of_clusters):
    for j in range(len(moved_reppoints[i])):
        for k in range(len(moved_reppoints[i][j])):
            moved_reppoints[i][j][k] = moved_reppoints[i][j][k] + (centroids[i][k] - moved_reppoints[i][j][k]) * p
			
data_points = np.loadtxt(dataFile, delimiter=',')
with open(outputFile, 'w') as f:
    for i in range(len(data_points)):
        clustnum = assign_clust(data_points[i], moved_reppoints)
        f.write(str(data_points[i].tolist()).replace("[", "").replace("]", "").replace(" ", "") + "," + str(clustnum) + "\n")