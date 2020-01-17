import time, json
from overlapGraph.generator import Randoms, RegionGenerator
from overlapGraph.slig.datastructs import Region,RegionSet,RIGraph,Interval


gen = RegionGenerator(dimension=2)

# regular json file
# gen.store_regionset(10000, "data/sample.json")

# spark-specific jsonl file
with open("data/sample.jsonl", "w") as outfile:
  for region in gen.get_regionset(10000):
    json.dump(region.to_dict(), outfile)
    outfile.write("\n")

#
# start_experiment = time.process_time()
# end_experiment = time.process_time()
# print(int(end_experiment - start_experiment))