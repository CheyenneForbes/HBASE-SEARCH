/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.search.LuceneProtocol.Results;
import org.apache.hadoop.hbase.search.LuceneProtocol.ScoreUID;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// nocommit: implement a test for:
//   Region flush
//   Region WAL restore
//   RPC search call
//   
public class TestLuceneCoprocessor {
  static final Log LOG = LogFactory.getLog(TestLuceneCoprocessor.class);
  static final String DIR = "test/build/data/TestLuceneCoprocessor/";
  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  static byte[] headersFam = Bytes.toBytes("headers");
  static byte[] to = Bytes.toBytes("to");
  static byte[] bodyFam = Bytes.toBytes("body");
  static byte[][] families = new byte[][] { headersFam, bodyFam };
  static byte[] messages = Bytes.toBytes("messages");
  static byte[] tableName = Bytes.toBytes("lucene");
  boolean deletesRan = false;
  /**
  @Test
  public void testAppendCodec() throws Exception {
    Directory directory = new RAMDirectory();
    IndexWriter writer = openWriter(directory);
    NumberFormat format = new DecimalFormat("00000");
    for (int x = 0; x < 20; x++) {
      //byte[] row = Bytes.toBytes(format.format(x));
      String row = format.format(x);
      // Put put = new Put(row);
      String s = "test hbase lucene";
      if (x % 2 == 0) {
        s += " apple";
      }
      Document doc = new Document();
      doc.add(new Field("text", s, Store.YES, Index.ANALYZED));
      doc.add(new Field("row", row, Store.YES, Index.NOT_ANALYZED));
      writer.addDocument(doc);
      // put.add(headersFam, to, Bytes.toBytes(s));
      // table.put(put);
    }
    writer.commit();
    IndexReader reader = IndexReader.open(directory, null, true, 32, writer
        .getConfig().getCodecProvider());

    Terms terms = MultiFields.getTerms(reader, "row");
    TermsEnum te = terms.iterator();
    //if (startTerm != null) {
    te.seek(new BytesRef(format.format(10)));
    while (true) {
      final BytesRef term = te.next();
      if (term == null) {
        break;
      } 
    }
    //}
    writer.close();
    directory.close();
  }
  **/
  protected IndexWriter openWriter(Directory directory) throws IOException {
    Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40,
        analyzer);
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    // compound files cannot be used with HDFS
    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setMergeScheduler(new SerialMergeScheduler());
    config.setCodecProvider(new HDFSCodecProvider());
    IndexWriter writer = new IndexWriter(directory, config);
    return writer;
  }

  /**
   * Perform a query using Lucene, then delete
   * the rows via HBase.  Then query Lucene
   * again to insure that the rows were
   * deleted there as well. 
   */
  @Test
  public void testSXDelete() throws Exception {
    HTable table = new HTable(util.getConfiguration(), tableName);
    
    byte[] row = Bytes.toBytes(Integer.toString(5));
    LuceneProtocol protocol = table.coprocessorProxy(LuceneProtocol.class, row);
    Results r = protocol.search("*:*", 10);
    Assert.assertEquals(20, r.numFound);
    
    Results results = protocol.search("to:apple", 10);
    Assert.assertEquals(10, results.numFound);
    for (ScoreUID uid : results.uids) {
      table.delete(new Delete(uid.row, uid.timestamp, null));
    }
    // all references to lucene should be removed
    Results results2 = protocol.search("to:apple", 10);
    Assert.assertEquals(0, results2.numFound);
    deletesRan = true;
  }
  
  /**
   * Tests flushing a region which is turn commits the Lucene index.
   */
  /**
  @Test
  public void testRegionCommit() throws Exception {
    HTable table = new HTable(util.getConfiguration(), tableName);
    NumberFormat format = new DecimalFormat("00000");
    /**
     * for (int x = 100; x < 200; x++) { byte[] row =
     * Bytes.toBytes(format.format(x)); Put put = new Put(row); String s =
     * "test hbase lucene"; if (x % 2 == 0) { s += " apple"; }
     * put.add(headersFam, to, Bytes.toBytes(s)); table.put(put); }
     **/
  /**
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        if (!Bytes.equals(r.getTableDesc().getName(), tableName)) {
          continue;
        }
        RegionCoprocessorHost cph = t.getRegionServer()
            .getOnlineRegion(r.getRegionName()).getCoprocessorHost();
        HRegion region = t.getRegionServer().getOnlineRegion(r.getRegionName());
        region.flushcache();
        
      }
    }
  }
  **/
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster(1);
    cluster = util.getMiniHBaseCluster();

    util.getTestFileSystem().delete(new Path("/"));

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.setValue("lucene.path", "TestLuceneCoprocessor");
    for (byte[] family : families) {
      tableDesc.addFamily(new HColumnDescriptor(family));
    }
    tableDesc.setValue("COPROCESSOR$1",
        String.format("/:%s:USER", LuceneCoprocessor.class.getName()));

    Configuration conf = util.getConfiguration();
    // conf.set("lucene.path", "TestLuceneCoprocessor");
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(tableDesc);
    HTable table = new HTable(conf, tableName);

    NumberFormat format = new DecimalFormat("00000");

    for (int x = 0; x < 20; x++) {
      byte[] row = Bytes.toBytes(format.format(x));
      Put put = new Put(row);
      String s = "test hbase lucene";
      if (x % 2 == 0) {
        s += " apple";
      }
      put.add(headersFam, to, Bytes.toBytes(s));
      table.put(put);
    }
    // table.close();
    /**
     * ResultScanner scanner = table.getScanner(headersFam);
     * System.out.println("rows:"); for (Result result : scanner) { String str =
     * Bytes.toString(result.getRow()); System.out.println(str); }
     * scanner.close();
     **/
    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    System.out.println("tearDownAfterClass");
    util.shutdownMiniCluster();
  }

  /**
   * Test search over the RPC protocol.
   */
  @Test
  public void testSearchRPC() throws Exception {
    byte[] row = Bytes.toBytes(Integer.toString(5));
    HTable table = new HTable(util.getConfiguration(), tableName);
    LuceneProtocol protocol = table.coprocessorProxy(LuceneProtocol.class, row);

    Results results = protocol.search("to:lucene", 10);
    //if (deletesRan) {
    Assert.assertEquals(20, results.numFound);
    //} else {
    //  Assert.assertEquals(20, results.numFound);
    //}
    
    results = protocol.search("to:apple", 10);
    Assert.assertEquals(10, results.numFound);
  }

  /**
   * Create a region, split it, then verify that the new 
   * indexes are correct.
   */
  @Test
  public void testRegionSplit() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    NumberFormat format = new DecimalFormat("00000");
    String num = format.format(new Integer(10));
    byte[] splitKey = Bytes.toBytes(num);
    admin.split(tableName, splitKey);
  }
  /**
  @Test
  public void testIndexSearcher() throws IOException {
    // System.out.println("testRegionObserver");
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        if (!Bytes.equals(r.getTableDesc().getName(), tableName)) {
          continue;
        }
        RegionCoprocessorHost cph = t.getRegionServer()
            .getOnlineRegion(r.getRegionName()).getCoprocessorHost();
        HRegion region = t.getRegionServer().getOnlineRegion(r.getRegionName());
        LuceneCoprocessor c = (LuceneCoprocessor) cph
            .findCoprocessor(LuceneCoprocessor.class.getName());

        IndexReader reader = IndexReader.open(c.getWriter(), true);
        try {
          HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader, region);

          assertTrue(c.getWriter() != null);

          TopDocs topDocs = searcher.search(new TermQuery(new Term("to",
              "lucene")), 100);
          Assert.assertEquals(20, topDocs.totalHits);

          topDocs = searcher
              .search(new TermQuery(new Term("to", "apple")), 100);
          Assert.assertEquals(10, topDocs.totalHits);
        } finally {
          reader.close();
        }
        // printDocs(region, c.writer);
      }
    }
  }
  **/
}
