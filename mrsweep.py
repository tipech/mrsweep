from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from copy import copy

from pprint import pprint


# ==== Execution Parameters ====

sample_fraction = 0.01  # 10% of 10000 = 100
num_partitions = 50


# ==== Setup ====

# create spark session with appropriate configuration
spark = SparkSession \
  .builder \
  .appName("MRSweep") \
  .getOrCreate()
  # .config("spark.executor.memory", "6g") \
  # .config("spark.executor.cores", "5") \
  # .config("spark.executor.instances", "20") \
  # .config("spark.driver.memory", "6g") \
  # .config("spark.driver.maxResultSize", "2g") \
spark.sparkContext.setLogLevel("ERROR")

# read the JSONL file
# points = sc.textFile("hdfs:///user/mahmoud2/2d_10mil_5pc_bimodal.txt")
regions = spark.read.json("file:///app/data/sample.jsonl").rdd


# ==== Sampling ====

# get a sample of dataset, determine number of dimensions
sample = regions.sample(False, sample_fraction).collect()
nr_dims = sample[0].dimension

# setup to partition points in each dimension
part_size  = int(len(sample)/num_partitions)
part_points = {} # in each dimension

# split to dimensions, get and sort region starting points in each
for d in range(nr_dims):
  sample_points = sorted(region.factors[d].lower for region in sample)

  # split sample to partitions
  part_points[d] = []
  previous_point = 0
  for i in range(num_partitions-1):
    next_point = sample_points[part_size*i]
    part_points[d].append((previous_point, next_point))
    previous_point = next_point
  part_points[d].append((previous_point, 1000))


# ==== Split data to dimensions ====

# transform into pairRDD (key-value pairs)
regions = regions.map(lambda x: (x.id, x))

# function that splits regions to their factor intervals
def dimension_split(pair):
  key, region = pair
  intervals = []
  for d in range(region.dimension):

    # each factor interval is based on a copy of the region object
    interval = copy(region)
    interval.dimension = d + 1

    # with a compound key and only the corresponding dimension's factor
    interval.id = "%s_%d"%(key,interval.dimension)
    interval.factors = region.factors[d]
    intervals.append(interval)

  # return (ID,interval) pairs
  return ((interval.id, interval) for interval in interval)

dim_regions = regions.flatMap(dimension_split)



pprint(dim_regions.collect()[:10])