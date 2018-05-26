import networkx as nx
import numpy as np
import sys
import os

inputfile = sys.argv[1]
iterations=int(sys.argv[2])
outputfile = sys.argv[3]

#inputfile='out.ego-facebook'
#outputfile='result.txt'
#iterations=3

data = np.loadtxt(inputfile,dtype=int)
graph_data = nx.Graph()

for i in data:
    graph_data.add_edge(*i)

clusters = []
flag=0

def second_smallest(eigenvalue):
    n1, n2 = float('inf'), float('inf')
    for i in eigenvalue:
        if i <= n1:
            n1, n2 = i, n1
        elif i < n2:
            n2 = i
    return n2

while (len(clusters) <iterations):
    negative_indices = []
    positive_indices = []
    negatives=[]
    positives=[]
          
    if flag!=0:
        index_large,length_large = float("-inf"),float("-inf")
        for i,value in enumerate(clusters):
            if len(value) > length_large:
                index_large = i
                length_large = len(value)
        graph1 = graph_data.subgraph(clusters[index_large])
        del clusters[index_large]
        
    else:
        graph1 = graph_data.copy()
        flag+=1
        
    L = nx.laplacian_matrix(graph1)
    eigenvalue,eigenvector = np.linalg.eig(L.todense())

    minval = second_smallest(eigenvalue)
    min_index = np.where(eigenvalue == minval)
    
    for i in range(len(eigenvector[:,min_index])):
        if eigenvector[:,min_index][i,0,0]<0:
            negative_indices.append(i)
        else:
            positive_indices.append(i)

    nodes = list(graph1.nodes)

    for i in negative_indices:
        negatives.append(nodes[i])
    for i in positive_indices:
        positives.append(nodes[i])

    clusters.append(negatives)
    clusters.append(positives)

    for i in negatives:
        for j in positives:
            if graph_data.has_edge(i,j):
                graph_data.remove_edge(i,j)

fp = open(outputfile,'w') 
for i in clusters:
    for j in sorted(i):
        fp.write(str(j)+",")
    fp.seek(-1, os.SEEK_CUR)
    fp.write('\n')
fp.seek(-1, os.SEEK_CUR)
fp.close()
