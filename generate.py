import time, json
from overlapGraph.generator import Randoms, RegionGenerator
from overlapGraph.slig.datastructs import Region,RegionSet,RIGraph,Interval


posnrng = Randoms.gauss(mean=0.5, sigma=0.2)
posnrng = Randoms.triangular(mode=0.5)
posnrng = Randoms.bimodal(mean1=0.2,sigma1=0.1, mean2=0.8,sigma2=0.1)
posnrng = Randoms.hotspot(n=5, d=2, min_mean=0, max_mean=1, min_sigma= 0.05,
  max_sigma= 0.1)
# posnrng = Randoms.uniform()

sizepc = Interval(0, 0.05)
sizepc = 0.01

gen = RegionGenerator(dimension=2,posnrng=posnrng,sizepc=sizepc,square=False)

# regular json file
# gen.store_regionset(1000, "data/sample.json")

# spark-specific jsonl file
with open("data/sample.jsonl", "w") as outfile:
  for i in range(1000):
    json.dump(gen.get_region().to_dict(), outfile)
    outfile.write("\n")

#
# start_experiment = time.process_time()
# end_experiment = time.process_time()
# print(int(end_experiment - start_experiment))