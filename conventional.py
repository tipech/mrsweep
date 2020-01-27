from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import json

from pprint import pprint


# ==== Execution Parameters ====

sample_fraction = 0.1  # 10% of 1000 = 100
num_partitions = 10

# ==== Setup ====

# create spark session with appropriate configuration
spark = SparkSession \
  .builder \
  .appName("MRSweep") \
  .config("spark.driver.memory", "6g") \
  .getOrCreate()
  # .config("spark.executor.memory", "6g") \
  # .config("spark.executor.cores", "5") \
  # .config("spark.executor.instances", "20") \
  # .config("spark.driver.maxResultSize", "2g") \
spark.sparkContext.setLogLevel("ERROR")

# read the JSONL file
# regions = spark.read.json("hdfs:///user/mahmoud2/data/sample.jsonl").rdd
regions = spark.read.json("file:///app/data/sample.jsonl").rdd

# accumulator for partition duplication
extras = spark.sparkContext.accumulator(0)


# ==== Sampling ====

# get a sample of dataset, determine number of dimensions
sample = regions.sample(False, sample_fraction).collect()
nr_dims = sample[0].dimension

# setup to partition points in each dimension
part_size  = len(sample)/num_partitions
part_points = {} # in each dimension

# split to dimensions, get and sort by region center points in each
for d in range(nr_dims):
  sample_points = sorted(
    (float(region.factors[d].lower) + float(region.factors[d].upper)) / 2
    for region in sample)

  # split sample to partitions
  part_points[d] = []
  previous_point = 0
  for i in range(num_partitions-1):
    next_point = sample_points[int(part_size*i)]
    part_points[d].append((previous_point, next_point))
    previous_point = next_point
  part_points[d].append((previous_point, 1000))


# ==== Split data to partitions ====

# transform into pairRDD (key-value pairs)
regions = regions.map(lambda x: (x.id, x))

# assign regions to partitions corresponding to their location
def partition(pair):
  region_id, region = pair
  partitions = [[]]
  
  # go through partition points in every dimension
  for d, points in part_points.items():
    new_partitions = []

    # find if this region starts in, ends in, or crosses over these points
    lower = float(region.factors[d][0])
    upper = float(region.factors[d][1])
    for i, part in enumerate(points):
      if ( (lower >= part[0] and lower <= part[1])
        or (upper >= part[0] and upper <= part[1])
        or (lower < part[0] and upper > part[1])):

        # add this partition to all partitions found in lower dimensions
        for combination in partitions:
          new_partitions.append(combination + [str(i)])


    # replace lower dimension partitions with current dimension combinations
    partitions = new_partitions

    # add number of extra copies of this region
    extras.add(len(partitions)-1)

  # partition id's are their index combinations per dimension
  return (('_'.join(part), region) for part in partitions)

# group objects to partitions
part_regions = regions.flatMap(partition).groupByKey()


# ==== Execute Sweep Line ====

# perform the sweep line loop, emit resulting intersection pairs
def sweep_line(pair):
  part_id, regions = pair

  # sort regions based on both their points in dimension 0
  points = ((p, r) for r in regions
    for p in (float(r.factors[0][0]), float(r.factors[0][1])))
  points = sorted(points, key=lambda x: x[0])

  actives = {}
  for coord, region in points:

    # region just ended, remove from actives
    if region.id in actives:
      del actives[region.id]

    # region just started, check other dimensions for intersection with active
    else:
      for active_id, active in actives.items():
        d=1
        while(d < region.dimension and overlaps(region, active, d)):
          d+=1

        if d == region.dimension:
          ids =list(sorted([region.id, active.id]))
          inter_id = "%s_%s" % (ids[0], ids[1])

          yield (inter_id, {"id": inter_id, "factors": [
            (max(region.factors[i][0], active.factors[i][0]), 
             min(region.factors[i][1], active.factors[i][1]))
            for i in range(d)]})

      # add to list of actives
      actives[region.id] = region


def overlaps(region, other, d):
  # check if two regions intersect in specified dimension

  this_f = region.factors[d]
  other_f = other.factors[d]
  return ( (this_f.lower >= other_f.lower and this_f.lower <= other_f.upper)
        or (this_f.upper >= other_f.lower and this_f.upper <= other_f.upper)
        or (this_f.lower < other_f.lower and this_f.upper > other_f.upper) )


# apply sweep line algorithm
part_intersects = part_regions.flatMap(sweep_line).groupByKey()

# Eliminate duplicates from partitioning
intersects = part_intersects.mapValues(lambda x: list(x)[0])

print(part_intersects.count())
print(intersects.count())

# print number of duplicates
print(extras)


# # ==== Store/Print Results ====

# # convert to jsonl format and save to file
# jsonl_intersects = intersects.map(json.dumps)
# # jsonl_intersects.saveAsTextFile("file:///app/data/result")

# # collect as list, sort (and print or save to file for comparison)
# result = list(sorted(jsonl_intersects.collect()))
# pprint(result[:10])
# # with open("test.txt", "w+") as file:
# #   file.write(str(result))

