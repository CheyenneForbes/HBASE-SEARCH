package org.apache.hadoop.hbase.search;

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

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

/**
 * An index searcher specifically for HBase.  
 */
public class HBaseIndexSearcher extends IndexSearcher {
  private HRegion region;

  public HBaseIndexSearcher(IndexReader r, HRegion region) {
    super(r);
    this.region = region;
  }

  public HBaseIndexSearcher(Directory path, boolean readOnly)
      throws CorruptIndexException, IOException {
    super(path, readOnly);
  }

  public HBaseIndexSearcher(Directory path) throws CorruptIndexException,
      IOException {
    super(path);
  }

  public HBaseIndexSearcher(IndexReader r, ExecutorService executor) {
    super(r, executor);
  }

  public HBaseIndexSearcher(IndexReader r) {
    super(r);
  }

  public HBaseIndexSearcher(ReaderContext context, ExecutorService executor) {
    super(context, executor);
  }

  public HBaseIndexSearcher(ReaderContext context) {
    super(context);
  }

  public Document doc(int docID) throws CorruptIndexException, IOException {
    IndexReader reader = getIndexReader();
    return loadRow(reader.document(docID), null);
  }

  public Document doc(int docID, FieldSelector fieldSelector)
      throws CorruptIndexException, IOException {
    IndexReader reader = getIndexReader();
    return loadRow(reader.document(docID), fieldSelector);
  }
  
  /**
   * Load the actual document data from HBase.
   */
  protected Document loadRow(Document d, FieldSelector fieldSelector) throws CorruptIndexException,
      IOException {
    Field uidField = d.getField(LuceneCoprocessor.UID_FIELD);
    String uid = uidField.stringValue();
    String[] split = StringUtils.split(uid, '?', '_');
    byte[] row = Bytes.toBytes(split[0]);
    long timestamp = Long.parseLong(split[1]);
    
    Get get = new Get(row);
    get.setTimeStamp(timestamp);
    
    Document doc = new Document();
    Result result = region.get(get, null);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result
        .getNoVersionMap();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> family : map.entrySet()) {
      NavigableMap<byte[], byte[]> value = family.getValue();
      for (Map.Entry<byte[],byte[]> columns : value.entrySet()) {
        String columnName = Bytes.toString(columns.getKey());
        if ( (fieldSelector == null) ||
            (fieldSelector != null && fieldSelector.accept(columnName).equals(FieldSelectorResult.LOAD))) {
          String columnValue = Bytes.toString(columns.getValue());
          Field field = new Field(columnName, columnValue, Store.YES, Index.ANALYZED);
          doc.add(field);
        }
      }
    }
    // add the uid field (required)
    doc.add(uidField);
    // add the row field (required)
    Field rowField = new Field("row", split[0], Store.YES, Index.NOT_ANALYZED);
    doc.add(rowField);
    return doc;
  }
}
