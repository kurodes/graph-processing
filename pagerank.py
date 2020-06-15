import sys
from pyspark import SparkConf, SparkContext
import time
import re

def computeContribs(dests, rank):
    num_dests = len(dests)
    for dest in dests: 
        yield (dest, rank / num_dests)


if __name__ == '__main__':

    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    ITER = 100
    d = 0.8
    N = 1000

    # First RDD: vertex adjacent list
    AdjacencyList = lines.map(lambda x: (x.split('\t')[0], x.split('\t')[1])).distinct().groupByKey().cache()

    # Second RDD: vertex rank value
    Ranks = AdjacencyList.map(lambda x: (x[0], 1.0/N)).cache()

    for iteration in range(0, ITER):
        contribs = AdjacencyList.join(Ranks).flatMap(lambda x: computeContribs(x[1][0], x[1][1]))
        Ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: rank * d + (1-d)/N)

    print(Ranks.top(5, key=lambda x: x[1]))

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
