from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import json

from pprint import pprint


# ==== Execution Parameters ====

sample_fraction = 0.01  # 10% of 10000 = 100
num_partitions = 50
algorithm = "sweep_line" # options: sweep_line,scan_line


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


# ==== Split data to dimensions ====

# transform into pairRDD (key-value pairs)
regions = regions.map(lambda x: (x.id, x))

# function that splits regions to their factor intervals
def split_dimensions(pair):
  region_id, region = pair
  intervals = []
  for d in range(region.dimension):

    # each factor interval is based on a copy of the region object
    interval = {"id": "%s_%d"%(region_id, d),
                "lower": float(region.factors[d][0]),
                "upper": float(region.factors[d][1]),
                "original": region_id, "d": d}
    intervals.append(interval)

  # return (ID,interval) pairs
  return ((interval['id'], interval) for interval in intervals)

dim_regions = regions.flatMap(split_dimensions)


# ==== Split data to partitions ====

# assign intervals to partitions corresponding to their location
def partition(pair):
  region_id, interval = pair
  part_intervals = []

  # for each partition, find objects that start in, end in, or cross over it
  for part_nr, part in enumerate(part_points[interval['d']]):
    if ( (interval['lower'] >= part[0] and interval['lower'] <= part[1])
      or (interval['upper'] >= part[0] and interval['upper'] <= part[1])
      or (interval['lower'] < part[0] and interval['upper'] > part[1])):

      part_intervals.append(("%d_%d"%(part_nr, interval['d']), interval))

  # return (partition_ID, interval) pairs
  return part_intervals

# group objects to partitions
part_regions = dim_regions.flatMap(partition).groupByKey()


# ==== Execute Sweep Line ====

# perform the sweep line loop, emit resulting intersection pairs
def sweep_line(pair):
  part_id, intervals = pair

  # sort intervals based on both their points
  points = ((p, i) for i in intervals for p in (i['lower'], i['upper']))
  points = sorted(points, key=lambda x: x[0])

  actives = {}
  for coord, interval in points:

    # interval just ended, remove from actives
    if interval['id'] in actives:
      del actives[interval['id']]

    # interval just started, it intersects all actives
    else:
      for active_id, active in actives.items():
        inter_id = "%s_%s"%(active['original'], interval['original'])
        yield (inter_id, {"id": inter_id,
              "lower": max(active['lower'], interval['lower']),
              "upper": min(active['upper'], interval['upper']),
              "d": active['d']})

      # add to list of actives
      actives[interval['id']] = interval


# apply sweep line algorithm
if algorithm == "sweep_line":
  dim_intersects = part_regions.flatMap(sweep_line).groupByKey()


# ==== Execute Scan Line ====

# sort same-partition intervals by start coord
sorted_regions = part_regions.mapValues(
  lambda x: sorted(x, key=lambda i:i['lower']))

# perform the scan line loop, emit resulting intersection pairs
def scan_line(pair):
  part_id, intervals = pair

  # go through intervals sequentially
  for i in range(len(intervals)):
    for k in range(i+1,len(intervals)):

      # new interval intersects current
      if intervals[i]['upper'] > intervals[k]['lower']:
        inter_id = "%s_%s"%(intervals[i]['original'],intervals[k]['original'])
        yield (inter_id, {"id": inter_id,
              "lower": max(intervals[i]['lower'], intervals[k]['lower']),
              "upper": min(intervals[i]['upper'], intervals[k]['upper']),
              "d": intervals[i]['d']})

      # no new intervals intersect current one, move on
      else:
        break

# apply scan line algorithm
if algorithm == "scan_line":
  dim_intersects = sorted_regions.flatMap(scan_line).groupByKey()

# ==== Merge intersections ====

# Find intersections that overlap in all dimensions
def merge(intervals):

  # remove duplicates made during partitioning by converting to dictionary
  factors = {i['d']: i for i in intervals}

  # mark those intersecting in fewer dimensions
  if len(factors) < nr_dims:
    return False

  # construct finalized intersections
  return {"id": factors[0]['id'],
          "lower": [i['lower'] for i in factors.values()],
          "upper": [i['upper'] for i in factors.values()]}

# apply reducer faction
intersects = dim_intersects.mapValues(merge).values().filter(lambda x: x)


# ==== Store/Print Results ====

# convert to jsonl format and save to file
jsonl_intersects = intersects.map(json.dumps)
# jsonl_intersects.saveAsTextFile("file:///app/data/result")

# collect as list, sort (and print or save to file for comparison)
result = list(sorted(jsonl_intersects.collect()))
pprint(result[:10])
# with open("test.txt", "w+") as file:
#   file.write(str(result))