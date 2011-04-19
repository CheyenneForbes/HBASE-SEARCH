/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.search;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

/**
 * Executes a benchmark comparing the query times against an index that is built
 * in HBase vs. the same index/set of documents stored on the local file system.
 * 
 * This benchmark is not for measuring the performance of indexing.
 */
public class TestSearchBenchmark {
  private static String[] queryStrings = {
      "states", "unit*", "uni*", "u*d", "un*d",
      "united~0.75", // 1
      "united~0.6", // 2
      "unit~0.7", // 1
      "unit~0.5", // 2
      "doctitle:/.*[Uu]nited.*/", "united OR states", "united AND states",
      "nebraska AND states", "\"united states\"", "\"united states\"~3", };
  int maxDocs = 10000;
  double ramBufferSize = 64.0;
  int numQueries = 50;
  int queryIterations = 3;
  Map<Query,Integer> queryResults = new HashMap<Query,Integer>();

  private long runQueries(int times, IndexSearcher searcher, Analyzer analyzer)
      throws Exception {
    long start = System.currentTimeMillis();
    QueryParser parser = new QueryParser(Version.LUCENE_40, "body", analyzer);
    parser.setLowercaseExpandedTerms(false);
    for (int x = 0; x < times; x++) {
      for (String q : queryStrings) {
        Query query = parser.parse(q);
        TopDocs topDocs = searcher.search(query, 10);
        Integer num = queryResults.get(query);
        if (num != null) {
          Assert.assertEquals((int)num, topDocs.totalHits);
        }
        queryResults.put(query, topDocs.totalHits);
      }
    }
    long duration = System.currentTimeMillis() - start;
    return duration;
  }

  private IndexWriterConfig createConfig() {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40,
        analyzer);
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    // compound files cannot be used with HDFS
    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setMergeScheduler(new SerialMergeScheduler());
    config.setRAMBufferSizeMB(ramBufferSize);
    config.setCodecProvider(new HDFSCodecProvider());
    return config;
  }
  
  public IndexSearcher buildHBaseIndex() throws Exception {
    IndexWriterConfig config = createConfig();

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();
    String dataDir = "/index";
    fileSys.mkdirs(new Path(dataDir));
    System.out.println("fileSys defaultReplication:"+fileSys.getDefaultReplication());
    System.out.println("fileSys class:"+fileSys.getClass().getName());

    HDFSDirectory dir = new HDFSDirectory(fileSys, dataDir);
    IndexSearcher searcher = buildIndex(dir, config);
    return searcher;
  }

  private IndexSearcher buildLuceneIndex() throws Exception {
    File luceneDir = new File("/tmp/luceneindex");
    luceneDir.mkdirs();
    FileUtils.cleanDirectory(luceneDir);
    Directory dir = new MMapDirectory(luceneDir);
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = createConfig();
    return buildIndex(dir, config);
  }

  private IndexSearcher buildIndex(Directory dir, IndexWriterConfig config)
      throws Exception {
    final IndexWriter writer = new IndexWriter(dir, config);
    loadDocs(maxDocs, new Loader() {
      public void doc(Document doc) throws Exception {
        writer.addDocument(doc);
      }
    });
    writer.commit();
    //for (String name : dir.listAll()) {
    //  System.out.println(name+":"+dir.fileLength(name));
    //}
    writer.close();
    IndexReader reader = IndexReader.open(dir, null, true, 32, config.getCodecProvider());
    return new IndexSearcher(reader);
  }
  
  @Test
  public void testLuceneQueries() throws Exception {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
    long luceneStart = System.currentTimeMillis();
    IndexSearcher luceneSearcher = buildLuceneIndex();
    long luceneDuration = System.currentTimeMillis() - luceneStart;
    System.out.println("lucene indexing duration:"+luceneDuration);
    try {
      for (int x = 0; x < queryIterations; x++) {
        long lucene = runQueries(numQueries, luceneSearcher, analyzer);
        System.out.println("lucene query time:" + lucene);
      }
    } finally {
      luceneSearcher.close();
    }
  }
  
  @Test
  public void testHBaseQueries() throws Exception {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);    
    long hbaseStart = System.currentTimeMillis();
    IndexSearcher hbaseSearcher = buildHBaseIndex();
    long hbaseDuration = System.currentTimeMillis() - hbaseStart;
    System.out.println("hbase indexing duration:"+hbaseDuration);
    try {
      for (int x = 0; x < queryIterations; x++) {
        long hbase = runQueries(numQueries, hbaseSearcher, analyzer);
        System.out.println("hbase query time:" + hbase);
      }
    } finally {
      hbaseSearcher.close();
    }
  }

  public static interface Loader {
    public void doc(Document doc) throws Exception;
  }

  public void loadDocs(int max, Loader loader) throws Exception {
    File dir = new File("wiki-en/10000");
    DocMaker docMaker = new DocMaker();
    Properties properties = new Properties();
    properties.setProperty("content.source",
        "org.apache.lucene.benchmark.byTask.feeds.DirContentSource");
    properties.setProperty("docs.dir", dir.getAbsolutePath());
    properties.setProperty("content.source.forever", "false");
    docMaker.setConfig(new Config(properties));
    docMaker.resetInputs();
    Document doc = null;
    int count = 0;
    try {
      while ((doc = docMaker.makeDocument()) != null) {
        if (count >= max)
          break;
        count++;
        loader.doc(doc);
      }
    } catch (NoMoreDataException e) {
      // continue
    }
    //long finish = System.currentTimeMillis();
    // System.out.println("Extraction took " + (finish - start) + " ms");
  }
}
