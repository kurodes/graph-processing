import sys
import time
import random

from pyspark import SparkConf, SparkContext


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)


def convert(edges, maps):
    # (K,V).join(K,W) = (K,(V,W))
    src = maps.join(edges).map(lambda x: (x[1][0], x[1][1]))
    des = maps.join(src.map(lambda x: (x[1], x[0]))).map(
        lambda x: (x[1][1], x[1][0]))
    return des

def convert_key(ranks, maps):
    return maps.join(ranks).map(lambda x: (x[1][0], x[1][1]))


def computeContribs(dests, rank):
    num_dests = len(dests)
    for dest in dests:
        yield (dest, rank / num_dests)


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    quiet_logs(sc)
    lines = sc.textFile(sys.argv[1])

    # pre-processing
    first1 = time.time()
    vertex_list = lines.flatMap(lambda x: x.split(' ')).distinct()
    vertex_num = vertex_list.count()
    v2id = vertex_list.zipWithIndex()
    # print(v2id.take(20))
    id2v = v2id.map(lambda x: (x[1], x[0]))

    if sys.argv[1] == 'wiki-Vote.txt':
        edges = lines.map(lambda x: (
            x.split(' ')[0], x.split(' ')[1])).distinct()
    elif sys.argv[1] == 'com-lj.ungraph.txt':
        edges = lines.map(lambda x: (x.split(' ')[0], x.split(' ')[1])).distinct(
        ).flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])
    cv_edges = convert(edges, v2id)
    # print(cv_edges.take(20))
    last1 = time.time()
    print("pre-processing time: %.2f seconds" % (last1 - first1))

    # graph processing
    if sys.argv[2] == 'pagerank':
        first2 = time.time()
        ITER = 20
        d = 0.8
        N = vertex_num
        # First RDD: vertex adjacent list
        AdjacencyList = cv_edges.groupByKey().cache()
        # Second RDD: vertex rank value
        Ranks = AdjacencyList.map(lambda x: (x[0], 1.0/N)).cache()
        for iteration in range(0, ITER):
            contribs = AdjacencyList.join(Ranks).flatMap(
                lambda x: computeContribs(x[1][0], x[1][1]))
            Ranks = contribs.reduceByKey(
                lambda x, y: x + y).mapValues(lambda rank: rank * d + (1-d)/N)
        top5 = Ranks.top(5, key=lambda x: x[1])
        print(top5)
        last2 = time.time()
        print("graph processing time: %.2f seconds" % (last2 - first2))

        # post-processing
        first3 = time.time()
        print("convert the node ID to raw identifier:")
        for i in top5:
            print(id2v.lookup(i[0])[0], i[1])
        last3 = time.time()
        print("post-processing time: %.2f seconds" % (last3 - first3))

        if sys.argv[1] == 'wiki-Vote.txt':
            v2id.coalesce(1,True).saveAsTextFile("pagerank-mapping.txt")

    elif sys.argv[2] == 'trustrank':
        first2 = time.time()
        ITER = 20
        d = 0.8
        N = vertex_num
        AdjacencyList = cv_edges.groupByKey().cache()
        whitelist = random.sample(range(0, N), 100)
        Ranks = AdjacencyList.map(lambda x: (x[0], 1.0/100) if x[0] in whitelist else (x[0], 0)).cache()
        for iteration in range(0, ITER):
            contribs = AdjacencyList.join(Ranks).flatMap(
                lambda x: computeContribs(x[1][0], x[1][1]))
            Ranks = contribs.reduceByKey(
                lambda x, y: x + y).mapValues(lambda rank: rank * d + (1-d)/N)
        Ranks.coalesce(1,True).saveAsTextFile("trustrank-graph-processing.txt")
        last2 = time.time()
        print("graph processing time: %.2f seconds" % (last2 - first2))

        # post-processing
        first3 = time.time()
        print("convert all node ID to raw identifier:")
        post_ranks = convert_key(Ranks, id2v)
        post_ranks.coalesce(1,True).saveAsTextFile("trustrank-post-processing.txt")
        last3 = time.time()
        print("post-processing time: %.2f seconds" % (last3 - first3))

        print(whitelist)
        v2id.coalesce(1,True).saveAsTextFile("trustrank-mapping.txt")
        

    sc.stop()
