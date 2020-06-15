import sys
import time
import random

from pyspark import SparkConf, SparkContext


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def convert(edges, maps):
    # (K,V).join(K,W) = (K,(V,W))
    src = maps.join(edges).map(lambda x: (x[1][0], x[1][1]))
    des = maps.join(src.map(lambda x: (x[1], x[0]))).map(
        lambda x: (x[1][1], x[1][0]))
    return des


def computeContribs(dests, rank):
    num_dests = len(dests)
    for dest in dests:
        yield (dest, rank / num_dests)


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    quiet_logs(sc)
    lines = sc.textFile(sys.argv[1])


    vertex_list = lines.flatMap(lambda x: x.split(' ')).distinct()

    # vertex_num = vertex_list.count()
    # v2id = vertex_list.zipWithIndex()
    # edges = lines.map(lambda x: (x.split(' ')[0], x.split(' ')[1])).distinct().flatMap(lambda x: [(x[0],x[1]),(x[1],x[0])])

    # print(v2id.collect())
    # print(edges.collect())

    # whitelist = random.sample(range(101, 105), 2)
    # print(whitelist)
    whitelist = ['101','102']
    Ranks = vertex_list.map(lambda x: (x, 1.0/100) if x in whitelist else (x, 0)).cache()

    print(Ranks.collect())

    sc.stop()

# spark-submit test.py test.txt