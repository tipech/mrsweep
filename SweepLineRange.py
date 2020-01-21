from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from operator import add
import pyspark
import sys

conf = SparkConf()
conf.setAppName("SweepLineRange")
conf.setAll([('spark.executor.memory', '6g'), ('spark.executor.cores', '5'), ('spark.executor.instances', '20'), ('spark.driver.memory','6g'), ('spark.driver.maxResultSize','2g')])
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# read the Text file
points = sc.textFile("hdfs:///user/mahmoud2/2d_10mil_5pc_bimodal.txt")
#hdfs:///user/mahmoud2/text_data_million.txt

# Take the list of the cordinations, and convert these cordination to integer values, so we can sort them at a later time.
def mapper(x,value):
	temp = []
	x=[float(i) for i in x]
	return(x,value)


def rangefunction(x, split_points):
	
	p = len(split_points)
	result = []
	
	for i in range(p-2):
		if  ( x[0][0] >= split_points[i] and x[0][0] < split_points[i+1] or x[0][1] >= split_points[i] and x[0][1] < split_points[i+1] or x[0][0] < split_points[i] and x[0][1] > split_points[i+1] ) :
			result.append([i,[x[0]]])
	if(x[0][0] >= split_points[p-1] or x[0][1] >= split_points[p-1] ):
		result.append([p,[x[0]]])
	
	return result
	


def SweepLine(key):
	SweepLinePoints= key[1]
	SweepLinePoints.sort()
	result = []
	print("length is = " + str(len(SweepLinePoints)))
	for i in range(0,len(SweepLinePoints)):
		for k in range(i+1,len(SweepLinePoints)):
					
			if SweepLinePoints[i][1] > SweepLinePoints[k][0]:
				#try:
				overlap=CaluclateRangeOverlap(SweepLinePoints[i],SweepLinePoints[k])	
				
				# if you want to save the intersections, then uncomment the line below, but make sure to give enough memory for the workers, or write the output to a file.
				# we are using list created inside the mapper to store all intersections; not rdds, therefore, there is a limit on the number of intersections we can save in memory, before we need to write them out to a file.
				yield ((SweepLinePoints[i],SweepLinePoints[k]),overlap)
				
				#except ValueError:
					#print(SweepLinePoints[i])
			else:
				break
	print("Finished  = " + str(len(SweepLinePoints)))
	return 	(key[0],result)


def CaluclateRangeOverlap(point1, point2):
	return ( max(point1[0],point2[0]), min(point1[1],point2[1]))
	



def check_Rdd_equality(rdd1,rdd2):
	rdd1= rdd1.collectAsMap()
	rdd2= rdd2.collectAsMap()
	return rdd1==rdd2

def f(x):
	return x
# process the cordinates, first split them, then convert them to integers, then map the cordinate of each intervel to value 1
points_temp = points.map(lambda line: line.split(" ")).map(lambda x : mapper(x,1))
#Sort
points_temp = points_temp.sortByKey(numPartitions=150)

# -------------partition
sample_size = 1000
sample = points_temp.takeSample(False, sample_size)
sample.sort()
num_partitions = 50
part_size  = int(sample_size/num_partitions)
split_points = [0]

for i in range(num_partitions-1):
	split_points.append(sample[part_size*i][0][1])
split_points.append(1000)

points_temp = points_temp.flatMap(lambda x: rangefunction(x, split_points) )

# reduce by key, will shuffle the data and mess up the sorting, therefore, we need to sort the points  again in the mapper.

points_temp = points_temp.reduceByKey(lambda a, b: a + b)

# ----------------End of partitioning

#SweepLine
print("starting SweepLine")
points_temp = points_temp.map(SweepLine)
print(points_temp.take(1))
print("ending Sweep line")


















#points_temp = points_temp.flatMapValues(f)

#points_temp = points_temp.map(lambda x: ((tuple(x[1][0][0]),tuple(x[1][0][1])),x[1][1]))
#print( points_temp.takeSample(False,20) )


#points_temp = points_temp.reduceByKey(lambda a, b: ( min(a[0],b[0]),max(a[1],b[1]) )   )
"""
print("Printing after reduction")
for i in points_temp.collect():
	print(i)
"""		
"""
points_temp = points_temp.sortByKey()
for i in points_temp.collect():
	print(i)
"""

#-------------------------------------------------------------

# run the sweep line alogrthim sequentially without partitioning  
"""
points_temp_without_partitioning= points.map(lambda line: line.split(" ")).map(lambda x : mapper(x,1)).sortByKey().flatMap(rangefunction_Without_partitioning).reduceByKey(lambda a, b: a + b).sortByKey()
points_temp_without_partitioning= points_temp_without_partitioning.repartition(1)
points_temp_without_partitioning= points_temp_without_partitioning.map(SweepLine)
points_temp_without_partitioning= points_temp_without_partitioning.flatMapValues(f).map(lambda x: ((tuple(x[1][0][0]),tuple(x[1][0][1])),x[1][1]))

points_temp_map= points_temp.collectAsMap()
points_temp_without_partitioning_map= points_temp_without_partitioning.collectAsMap()
print("---------")

print("Compaing points_temp_map to points_temp_without_partitioning_map")
x=0
for i in points_temp_map:
	if i not in points_temp_without_partitioning_map and (i[1],i[0]) not in  points_temp_without_partitioning_map :
		x+=1
		print(i[0])
		print(points_temp_map[i])
print(x)
y=0
print("Compaing points_temp_without_partitioning_map to points_temp_map")
for i in points_temp_without_partitioning_map:
	if i not in points_temp_map and  (i[1],i[0]) not in points_temp_map: 
		y+=1
		print(i)
		print(points_temp_without_partitioning_map[i])
print(y)
#-----------------------------------------------

#compare the result
print("is the result return by the distrubted sweep line correct (compare the output returned by the distributed sweep line with the output of a sweep line run in non-distributed way )? " + str(x==y))
#points_temp_without_partitioning= points_temp_without_partitioning.flatMapValues(f).map(lambda x: ((tuple(x[1][0][0]),tuple(x[1][0][1])),x[1][1])).reduceByKey(lambda a, b: ( min(a[0],b[0]),max(a[1],b[1]) )   ).sortByKey()


#points_temp = points_temp.sortByKey()
#for i in points_temp.collect():
#	print(i)
""" 
