import os
from sets import Set
import re
from pyspark import SparkContext
from operator import add
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import string


sc = SparkContext(appName = 'inf551')

file = sys.argv[1]

lines = sc.textFile(file)


def replace_punc(events,count):

            eventname=events.encode('ascii','ignore')
            set1=Set(string.punctuation)
            set2=Set("'-")
            punc=set1-set2 #Handling all punctuations except apostrophes and dash
            punc=''.join(punc)
            translator = string.maketrans(punc, " "* len(punc))
            lines1=eventname.translate(translator)
            lines2=re.sub("-","",lines1)
            lines2=re.sub("\'","",lines2)
            lines3=re.sub("\s+"," ",lines2)
            lines4=lines3.strip()
            eventname=lines4.lower().strip()
            if eventname!="":
                return eventname,count
            return None,count

header=lines.first()
data=lines.filter(lambda line:line!=header)
data_values=data.map(lambda x:x.split(","))
columns=data_values.map(lambda x: (x[3] if x[3] is not None else None,float(x[18])))
reqd_data=columns.filter(lambda x:x!=None)
clean_data= reqd_data.map(lambda x: (replace_punc(x[0],x[1]))).filter(lambda x:x!=None)

rddDict = clean_data.countByKey()
output_list = clean_data.reduceByKey(add).collect()


#print output_list
for i in range(len(output_list)):
        rddDict[output_list[i][0]] = [rddDict[output_list[i][0]],format(float(output_list[i][1]/rddDict[output_list[i][0]]),'0.3f')]

output = sorted(rddDict.items(), key = lambda x:x[0])
#print len(output)

if not os.path.exists(sys.argv[2]):
   os.makedirs(sys.argv[2])

f=open(sys.argv[2]+'/Roshani_Mangalore_task2.txt','w+')
for i in output:

    if i[0] is not None:
        #print (i[0] + "\t"+str(i[1][0])+"\t"+ str(i[1][1]).decode('ascii', 'ignore'))
        f.write(i[0]+"\t"+str(i[1][0])+"\t"+ str(i[1][1]).decode('ascii','ignore')+"\n")


