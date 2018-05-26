import os, sys, operator
from itertools import chain, combinations
from collections import defaultdict
from pyspark import SparkContext
import math
from operator import add

file = sys.argv[1]
s = float(sys.argv[2])
outputfile = sys.argv[3]
sc = SparkContext(appName = 'inf553')

def generateCandidates(frequentSet, itemsize):
    output=[]
    allItems = set([item for temp in frequentSet for item in temp])
    if len(allItems)>= itemsize:
        itemCombinations = chain(*[combinations(allItems, itemsize)])
        for itemCombination in itemCombinations:
            subsetCombinations = chain(*[combinations([item for item in itemCombination], itemsize - 1)])
            allSubsetsFrequent = True
            for subset in subsetCombinations:
                if tuple(sorted(subset)) not in frequentSet:
                    allSubsetsFrequent = False
                    break
            if allSubsetsFrequent:
                output.append(tuple(sorted(itemCombination)))        
    return output

def getFrequentItems(candidateSet, basket,minSupport):
	output = defaultdict(int)
	itemsetFrequency = defaultdict(int)
	if candidateSet:
		basket = [item for item in basket if len(item)>=len(max(candidateSet))]
        
        if len(basket)<minSupport:
            return output
        for item in candidateSet:
            for item_list in basket:
                    if set(item).issubset(set(item_list)):
                        if itemsetFrequency[tuple(sorted(item))] == minSupport:
                            output[tuple(sorted(item))] = minSupport
                            break
                        else :
                            itemsetFrequency[tuple(sorted(item))] += 1
        return output

def getFrequency(candidateSet, basket):
    itemsetFrequency = defaultdict(int)
    if candidateSet:
        basket = [item for item in basket if len(item)>=len(max(candidateSet))]
        for item in candidateSet:
            for item_list in basket:
                    if set(item).issubset(set(item_list)):
                        itemsetFrequency[tuple(sorted(item))] += 1
    return itemsetFrequency

def son_phase1(item_iter):
    freqset =[]
    singletons = defaultdict(int)
    global minSupport,itemsize,candidates
    itemList = list()
    itemSet = set()
    for item_list in item_iter:
        itemList.append(item_list)
        if itemsize ==1 :
            for item in item_list:
                singletons[item]+=1
                itemSet.add(item)

    minSupport = math.floor(minSupport * len(itemList)/total_count) 
    if itemsize == 1 :
        itemSet = {k:v for k,v in singletons.items() if v>=minSupport}
        itemSet = [itemset for itemset in chain(*[combinations(itemSet, 1)])] 
        return itemSet
    if itemsize >1:
        candidates = generateCandidates(candidates, itemsize)
        itemSet = getFrequentItems(candidates, itemList,minSupport)
        return itemSet.keys()
        
def son_phase2(item_iter):
    global minSupport,candidates
    output=[]
    itemList=list()
    for item_list in item_iter:
        itemList.append(item_list)
    
    minSupport = math.floor(minSupport * len(itemList)/total_count)
    candidateSetFrequency = getFrequency(candidates,itemList)
    for key,value in candidateSetFrequency.items():
        output.append(tuple((key,value)))
    return output
			
def display(freqset):
    output = open(outputfile,'w')
    frequentItemSets= defaultdict(list)
    for x in freqset:
        frequentItemSets[len(x)].append(x)
    for key,value in frequentItemSets.items():
        if key==1:
            for x in sorted(value):
                output.write(str(x[0]))
                output.write("\n")
        else :
            for x in sorted(value):
                res = ''
                x = str(x)
                x = x.lstrip('(')
                x = x.rstrip(')')
                temp = x.split(',')
                res = '('
                for x in temp:
                    res+=str(x.strip())+','
		res.strip(',')
                res += ')'
                output.write(res)
                output.write("\n")


final_output = []
data= sc.textFile(file).map(lambda line : line.encode('ascii', 'ignore').split(","))
total_count = data.count()
minSupport = s*total_count
itemsize = 1
candidates =[]
while candidates or itemsize==1:
        candidates = data.mapPartitions(son_phase1).distinct().collect()
        finalcandidates= data.mapPartitions(son_phase2).reduceByKey(add)
	finalcandidates=finalcandidates.filter(lambda x: x[1]>=minSupport).map(lambda line: line[0]).collect()
        for keys in finalcandidates:
            final_output.append(keys)
        itemsize+=1

final_output = sorted(final_output)
final_output.sort(key= lambda t :len(t),reverse=False)
display(final_output)
