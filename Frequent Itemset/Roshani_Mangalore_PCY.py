import os
from pyspark import SparkContext
from operator import add
from collections import defaultdict
import itertools
import sys
from decimal import *
sc = SparkContext(appName = 'inf553')

#Read input parametres
file = sys.argv[1]
a = int(sys.argv[2])
b = int(sys.argv[3])
N = int(sys.argv[4])
s = int(sys.argv[5])

#count table
def key_count(l):
    for i in l:
        keycount[i[0]] = int(i[1])

def generate_pairs(s,a,b,N):
     values_split = map(int, s.split(','))
     values_pairs = list(itertools.combinations(values_split, 2))
     output = []
     for i in values_pairs:
        hash_function = (a*i[0] + b*i[1]) % N     
        output.append((hash_function,i))    
     return output

def generate_frequent_pairs(s):
    values_pairs = list(itertools.combinations(map(int,s.split(',')),2))
    output_freq_pairs=[]
    for i in values_pairs:
        hashfunction=(i[0]*a+i[1]*b)%N
        if freq_basket_bitmap[hashfunction]==1 and keycount[i[0]]==1 and keycount[i[1]]==1:
            output_freq_pairs.append((hashfunction,i))
    return output_freq_pairs

def pruned_candidate_pairs(l):
     temp = list(itertools.combinations(map(int, l.split(',')),2))
     pruned_candidates = []
     for x in temp:
        if freq_basket_bitmap[(x[0]*a + x[1]*b) % N] == 0 and keycount[x[0]] == 1 and keycount[x[1]] == 1:
            pruned_candidates.append(x)
     return pruned_candidates
            
#create count table and bitmap table    
data = sc.textFile(file)
data_split = data.flatMap(lambda x:x.split(','))
item_map=data_split.map(lambda x: (int(x.encode('ascii','ignore')), 1))
count_table = item_map.reduceByKey(add)

keycount = defaultdict(int)
frequent_items=[]
key_count(count_table.collect())
for key,count in keycount.iteritems():
    if count>=s:
        keycount[key]=1
        frequent_items.append(key)
    else:
        keycount[key]=0

#create hash table and bitmap hashtable for pass1
data1=sc.textFile(file)
map_pairs = data1.flatMap(lambda x: generate_pairs(x, a, b, N))

freq_basket_bitmap = map_pairs.countByKey()
for hash_key,count in freq_basket_bitmap.iteritems():
    if count>=s:
        freq_basket_bitmap[hash_key]=1
    else:
        freq_basket_bitmap[hash_key]=0
 
data2=sc.textFile(file)
map_candidate_pairs = data2.flatMap(lambda x:generate_frequent_pairs(x))
count1=0
freqbuckets=map_candidate_pairs.countByKey()
for hashkey,k in freqbuckets.iteritems():
    if k>=s:
        freqbuckets[hashkey]=1
        count1+=1
    else:
        freqbuckets[hashkey]=0
      
pairs_count = map_candidate_pairs.map(lambda x : (x[1],1))
pairs_count=pairs_count.reduceByKey(add).sortByKey()

data3=sc.textFile(file)
map_pruned_candidates=data3.flatMap(lambda x: pruned_candidate_pairs(x))

pruned_candidates=map_pruned_candidates.map(lambda x:(x,1))
pruned_candidates=pruned_candidates.reduceByKey(add).sortByKey().map(lambda x:(x[0][0],x[0][1])).collect()

false_positives_list = pairs_count.filter(lambda x: x[1] < s).map(lambda x:(x[0][0],x[0][1])).map(lambda x:(x)).sortByKey().collect()
true_positives_list  = pairs_count.filter(lambda x: x[1] >= s).map(lambda x:(x[0][0],x[0][1])).sortByKey().collect()

frequent_pairs=pairs_count.map(lambda x: (x[0][0],x[0][1])).collect()
frequents=frequent_items+true_positives_list

false_positive_rate=format(float(count1)/N,'0.3f')
print "False Positive Rate:"+str(false_positive_rate)

#create output directory and output files
if not os.path.exists(sys.argv[6]):
   os.makedirs(sys.argv[6])

ff=open(sys.argv[6]+'/frequentset.txt','w+')
for i in frequents:
    ff.write(str(i).replace(" ","")+"\n")

fc=open(sys.argv[6]+'/candidates.txt','w+')
for i in pruned_candidates:
    fc.write(str(i).replace(" ","")+"\n")

