README for HBase Search

-- HDFS --

First the customized HDFS Append branch must be downloaded from git and Maven installed locally:

git clone https://jasonrutherglen@github.com/jasonrutherglen/HDFS-347-HBASE.git HBASE-347-HBASE

cd HBASE-347-HBASE

mvn install

-- LUCENE -- 



--BENCHMARK--

To execute the benchmark, download the following wikipedia entries from:
http://people.apache.org/~gsingers/wikipedia/enwiki-20070527-pages-articles.xml.bz2 to the root of the project directory
using 'curl -O http://people.apache.org/~gsingers/wikipedia/enwiki-20070527-pages-articles.xml.bz2'

To execute the benchmark against Lucene run: 

mvn test -Dtest=TestSearchBenchmark

The output will be in the file: target/surefire-reports/org.apache.hadoop.hbase.search.TestSearchBenchmark-output.txt

Multiple rounds of queries are executed, the 2nd and 3rd should be very close.


