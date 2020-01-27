from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import json

from pprint import pprint


# ==== Execution Parameters ====

sample_fraction = 0.1  # 10% of 1000 = 100
num_partitions = 1


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
        while(d < region.dimension and intersects(region, active, d)):
          d+=1

        if d == region.dimension:
          inter_id = "%s_%s"%(region.id, active.id)

          yield (inter_id, {"id": inter_id, "factors": [
            (max(region.factors[i][0], active.factors[i][0]), 
             min(region.factors[i][1], active.factors[i][1]))
            for i in range(d)]})

      # add to list of actives
      actives[region.id] = region


def intersects(region, other, d):
  # check if two regions intersect in specified dimension

  this_f = region.factors[d]
  other_f = other.factors[d]
  return ( (this_f[0] >= other_f[0] and this_f[0] <= other_f[1])
        or (this_f[1] >= other_f[0] and this_f[1] <= other_f[1])
        or (this_f[0] < other_f[0] and this_f[1] > other_f[1]) )


# apply sweep line algorithm
intersects = part_regions.flatMap(sweep_line).groupByKey()

print(intersects.count())


# # ==== Execute Scan Line ====

# # sort same-partition intervals by start coord
# sorted_regions = part_regions.mapValues(
#   lambda x: sorted(x, key=lambda i:i['lower']))

# # perform the scan line loop, emit resulting intersection pairs
# def scan_line(pair):
#   part_id, intervals = pair

#   # go through intervals sequentially
#   for i in range(len(intervals)):
#     for k in range(i+1,len(intervals)):

#       # new interval intersects current
#       if intervals[i]['upper'] > intervals[k]['lower']:
#         inter_id = "%s_%s"%(intervals[i]['original'],intervals[k]['original'])
#         yield (inter_id, {"id": inter_id,
#               "lower": max(intervals[i]['lower'], intervals[k]['lower']),
#               "upper": min(intervals[i]['upper'], intervals[k]['upper']),
#               "d": intervals[i]['d']})

#       # no new intervals intersect current one, move on
#       else:
#         break

# # apply scan line algorithm
# if algorithm == "scan_line":
#   dim_intersects = sorted_regions.flatMap(scan_line).groupByKey()

# # ==== Merge intersections ====

# # Find intersections that overlap in all dimensions
# def merge(intervals):

#   # remove duplicates made during partitioning by converting to dictionary
#   factors = {i['d']: i for i in intervals}

#   # mark those intersecting in fewer dimensions
#   if len(factors) < nr_dims:
#     return False

#   # construct finalized intersections
#   return {"id": factors[0]['id'],
#           "lower": [i['lower'] for i in factors.values()],
#           "upper": [i['upper'] for i in factors.values()]}

# # apply reducer faction
# intersects = dim_intersects.mapValues(merge).values().filter(lambda x: x)


# # ==== Store/Print Results ====

# # convert to jsonl format and save to file
# jsonl_intersects = intersects.map(json.dumps)
# # jsonl_intersects.saveAsTextFile("file:///app/data/result")

# # collect as list, sort (and print or save to file for comparison)
# result = list(sorted(jsonl_intersects.collect()))
# pprint(result[:10])
# # with open("test.txt", "w+") as file:
# #   file.write(str(result))