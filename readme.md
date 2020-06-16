hadoop fs -ls hdfs://bd2:9000/user/2019310825/

hadoop fs -rm -r -f hdfs://bd2:9000/user/2019310825/pagerank*
hadoop fs -get hdfs://bd2:9000/user/2019310825/pagerank*

hadoop fs -rm -r -f hdfs://bd2:9000/user/2019310825/trustrank*
hadoop fs -get hdfs://bd2:9000/user/2019310825/trustrank*

./run.sh wiki-Vote.txt pagerank
./run.sh wiki-Vote.txt trustrank

./run.sh com-lj.ungraph.txt pagerank


