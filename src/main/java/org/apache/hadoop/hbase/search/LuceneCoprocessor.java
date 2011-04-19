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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserverCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PKIndexSplitter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class LuceneCoprocessor extends BaseRegionObserverCoprocessor implements
    LuceneProtocol {
  public static final Log LOG = LogFactory.getLog(LuceneCoprocessor.class);
  private IndexWriter writer;
  public static final String ROW_FIELD = "row";
  public static final String UID_FIELD = "uid"; // rowid_timestamp
  private DocumentTransformer documentTransformer = new DefaultDocumentTransformer();
  private HRegion region;
  private HDFSDirectory directory;
  private String lucenePath;

  static {
    CodecProvider.setDefault(new HDFSCodecProvider());
  }

  /**
   * Delete rows from this region based on the Lucene query.
   */
  /**
  public void delete(String queryString) throws Exception {
    Query query = parseQuery(queryString);
    IndexReader reader = IndexReader.open(writer, true);
    try {
      HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader);
      Collector collector = new DeleteHitCollector();
      writer.deleteDocuments(query);
      searcher.search(query, collector);
    } finally {
      reader.close();
    }
  }
  **/
  protected Query parseQuery(String queryString) throws Exception {
    QueryParser qp = new QueryParser(Version.LUCENE_40, ROW_FIELD,
        writer.getAnalyzer());
    Query query = qp.parse(queryString);
    return query;
  }

  /**
   * Collector of documents that deletes from the underlying HRegion.
   */
  /**
  private class DeleteHitCollector extends Collector {
    private IndexReader current;
    private HBaseIndexSearcher searcher;

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) {
      try {
        Document document = current.document(doc);
        String uid = document.get(LuceneCoprocessor.UID_FIELD);
        String[] split = StringUtils.split(uid, '?', '_');
        byte[] row = Bytes.toBytes(split[0]);
        long timestamp = Long.parseLong(split[1]);
        Delete delete = new Delete(row);
        delete.setTimestamp(timestamp);
        region.delete(delete, null, true);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      current = context.reader;
      searcher = new HBaseIndexSearcher(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }
  **/
  /**
   * Perform a query based on the given query string. Returns results up to the
   * given hits number.
   */
  public Results search(String queryString, int hits) throws Exception {
    IndexReader reader = IndexReader.open(writer, true);
    try {
      Query query = parseQuery(queryString);
      HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader, region);
      TopDocs top = searcher.search(query, null, hits);
      ScoreUID[] uids = new ScoreUID[top.scoreDocs.length];
      for (int x = 0; x < uids.length; x++) {
        Document document = reader.document(top.scoreDocs[x].doc);
        String uid = document.get(LuceneCoprocessor.UID_FIELD);
        String[] split = StringUtils.split(uid, '?', '_');
        byte[] row = Bytes.toBytes(split[0]);
        long timestamp = Long.parseLong(split[1]);
        uids[x] = new ScoreUID(top.scoreDocs[x].score, row, timestamp);
      }
      return new Results(top.totalHits, uids);
    } finally {
      reader.close();
    }
  }

  public static String toUID(String row, long timestamp) {
    return row + "_" + timestamp;
  }

  public IndexWriter getWriter() {
    return writer;
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return 0L;
  }

  /**
   * Split the underlying Lucene index .
   */
  // nocommit: We need a test to see if HBase flushes/commits
  // prior to splitting. If HBase splits prior to
  // flushing, then need to know which IndexReader on the
  // current HRegion's index to use for splitting.
  @Override
  public void postSplit(final RegionCoprocessorEnvironment e, final HRegion l,
      final HRegion r) {
    try {
      FileSystem filesystem = e.getRegion().getFilesystem();
      
      byte[] splitKey = r.getStartKey();
      Term splitTerm = new Term(ROW_FIELD, new BytesRef(splitKey));

      String origPath = getLucenePath(e.getRegion());

      LOG.info("region:" + e.getRegion().getRegionNameAsString() + " l:"
          + l.getRegionNameAsString() + " r:" + r.getRegionNameAsString());
      
      String dir1Path = getLucenePath(l);
      filesystem.mkdirs(new Path(dir1Path));
      // if (filesystem.exists(new Path(dir1Path))) {
      // throw new IOException("dir1Path:"+dir1Path+" exists");
      // }

      IndexReader reader = IndexReader.open(directory, null, true,
          32, writer.getConfig().getCodecProvider());
      Terms terms = MultiFields.getTerms(reader, ROW_FIELD);
      try {
        TermsEnum te = terms.iterator();
        te.seek(new BytesRef(splitKey));
        while (true) {
          BytesRef ref = te.next();
          if (ref == null) break;
        }
      } finally {
        //terms.close();
      }

      HDFSDirectory dir1 = new HDFSDirectory(filesystem, dir1Path);

      String dir2Path = getLucenePath(r);
      filesystem.mkdirs(new Path(dir2Path));
      // if (filesystem.exists(new Path(dir2Path))) {
      // throw new IOException("dir2Path:"+dir2Path+" exists");
      // }

      HDFSDirectory dir2 = new HDFSDirectory(filesystem, dir2Path);
      LOG.info("split begin:" + Bytes.toStringBinary(splitKey) + " region:"
          + e.getRegion().getRegionNameAsString() + " left:"
          + l.getRegionNameAsString() + " right:" + r.getRegionNameAsString());

      PKIndexSplitter indexSplitter = new PKIndexSplitter(splitTerm, directory,
          dir1, dir2, writer.getConfig());
      indexSplitter.split();
      LOG.info("split end:" + Bytes.toStringBinary(splitKey));

      System.out.println("writer1 docs:");
      IndexWriter writer1 = openWriter(dir1Path, filesystem);
      printDocs(l, writer1);
      writer1.close();

      System.out.println("writer2 docs:");
      IndexWriter writer2 = openWriter(dir2Path, filesystem);
      printDocs(r, writer2);
      writer2.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static void printDocs(HRegion region, IndexWriter writer)
      throws IOException {
    IndexReader reader = IndexReader.open(writer, true);
    try {
      HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader, region);
      int num = reader.maxDoc();
      for (int x = 0; x < num; x++) {
        Document doc = reader.document(x);
        Document sdoc = searcher.doc(x);
        System.out.println("doc" + x + ":" + doc + " sdoc:" + sdoc);
      }
    } finally {
      reader.close();
    }
  }

  @Override
  public void preOpen(RegionCoprocessorEnvironment e) {
  }

  public String getLucenePath(HRegion region) {
    // String lucenePath = region.getConf().get("lucene.path");
    String name = region.getRegionNameAsString();
    name = name.replace(',', '_');
    name = name.replace('.', '_');
    // String encodedName = region.getRegionInfo().getEncodedName();
    String regionLucenePath = lucenePath + "/" + name;
    return regionLucenePath;
  }

  protected IndexWriter openWriter(String path, FileSystem fileSystem)
      throws IOException {
    Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40,
        analyzer);
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    // compound files cannot be used with HDFS
    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setMergeScheduler(new SerialMergeScheduler());
    config.setCodecProvider(new HDFSCodecProvider());
    fileSystem.mkdirs(new Path(path));
    HDFSDirectory directory = new HDFSDirectory(fileSystem, path);
    IndexWriter writer = new IndexWriter(directory, config);
    return writer;
  }

  /**
   * Initialize Lucene's IndexWriter.
   */
  @Override
  public void postOpen(RegionCoprocessorEnvironment e) {
    try {
      region = e.getRegion();
      lucenePath = e.getRegion().getTableDesc().getValue("lucene.path");
      String regionName = region.getRegionNameAsString();
      FileSystem fileSystem = region.getFilesystem();

      String regionLucenePath = getLucenePath(e.getRegion());

      //if (fileSystem.exists(new Path(regionLucenePath))) {
      //  throw new IOException("Region Lucene path exists:" + regionLucenePath);
      //}

      writer = openWriter(regionLucenePath, fileSystem);
      directory = (HDFSDirectory) writer.getDirectory();

      LOG.info("Index opened for region:" + regionName);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void preClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
  }

  @Override
  public void postClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
    try {
      writer.close();
      LOG.info("IndexWriter closed:" + e.getRegion().getRegionNameAsString());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void postWALRestore(RegionCoprocessorEnvironment env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    List<KeyValue> kvs = logEdit.getKeyValues();
    Collection<Term> delTerms = getDeletes(kvs);
    writer.deleteDocuments((Term[]) delTerms.toArray(new Term[0]));

    Document doc = documentTransformer.transform(this, kvs);
    Term rowTerm = getRowTerm(kvs.get(0));
    writer.updateDocument(rowTerm, doc);
  }

  private Collection<Term> getDeletes(List<KeyValue> kvs) {
    Set<Term> terms = new HashSet<Term>();
    for (KeyValue kv : kvs) {
      if (kv.isDelete()) {
        terms.add(getRowTerm(kv));
      }
    }
    return terms;
  }
  /**
  private Term getUIDTerm(KeyValue kv) {
    Bytes.
    BytesRef uid = new BytesRef(Bytes);
    return new Term(UID_FIELD, uid);
  }
  **/
  private Term getRowTerm(KeyValue kv) {
    BytesRef row = new BytesRef(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    return new Term(ROW_FIELD, row);
  }

  /**
   * Delete from IndexWriter.
   */
  @Override
  public void postDelete(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    try {
      Term delTerm = getRowTerm(familyMap);
      writer.deleteDocuments(delTerm);
      LOG.info("postDelete:" + delTerm);
    } catch (CorruptIndexException cie) {
      throw new IOException("", cie);
    }
  }

  private Term getRowTerm(Map<byte[], List<KeyValue>> familyMap) {
    for (Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
      KeyValue kv = entry.getValue().get(0);
      return getRowTerm(kv);
    }
    return null;
  }

  /**
   * Add a document to IndexWriter.
   */
  @Override
  public void postPut(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    // nocommit: we will build up too many delete terms if we
    // always try to delete the previous document
    // which may not actually be there
    // however looking up each document
    // could be expensive, though with some of the
    // new FST work, it could become cheaper (see LUCENE-2948)
    Term rowTerm = getRowTerm(familyMap);
    Document doc = documentTransformer.transform(this, familyMap);
    if (doc != null) {
      if (rowTerm != null) {
        writer.updateDocument(rowTerm, doc);
      } else {
        writer.addDocument(doc);
      }
    }
  }

  @Override
  public void preFlush(RegionCoprocessorEnvironment e) {
    LOG.info("preFlush:" + region.getRegionNameAsString());
  }

  @Override
  public void postFlush(RegionCoprocessorEnvironment e) {
    try {
      HRegion region = e.getRegion();
      long flushTime = region.getLastFlushTime();
      Map<String, String> commitUserData = new HashMap<String, String>();
      commitUserData.put("flushTime", Long.toString(flushTime));
      writer.commit(commitUserData);
      LOG.info("postFlush commit:" + region.getRegionNameAsString());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
