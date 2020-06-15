import re
import sys
from pyspark import SparkConf, SparkContext
import time

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # Students: Implement Word Count!
    words = lines.flatMap(lambda x: re.split('[^\w]+', x))
    wordskv = words.map(lambda x: (x, 1))
    wordscount = wordskv.reduceByKey(lambda a, b: a + b)
    print(wordscount.takeOrdered(10, key=lambda x: -x[1]))
    # print(wordscount.top(10, key=lambda x: x[1]))

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
