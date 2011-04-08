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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

/**
 * A DocumentTransformer implementation that uses a KeyValue's
 * qualifier as the Lucene Document field name.  All qualifiers
 * across column families are added to a single Document.
 */
// nocommit: this DocumentTransformer assumes that
// the incoming KeyValue(s) are for the entire row
// we may need to load the [entire] row from the 
// HRegion
public class DefaultDocumentTransformer implements DocumentTransformer {
  @Override
  public Document transform(LuceneCoprocessor lucene, List<KeyValue> kvs)
      throws IOException {
    byte[] row = null;
    long timestamp = -1;
    Document doc = new Document();
    for (KeyValue kv : kvs) {
      if (row == null) {
        row = kv.getRow();
        timestamp = kv.getTimestamp();
      }
      
      // the row should always be the same
      assert Arrays.equals(row, kv.getRow());
      
      String name = Bytes.toStringBinary(kv.getQualifier());
      String value = Bytes.toStringBinary(kv.getValue());
      Field field = new Field(name, value, Store.NO, Index.ANALYZED);
      doc.add(field);
    }
    addFields(row, timestamp, doc, lucene);
    return doc;
  }

  @Override
  public Document transform(LuceneCoprocessor lucene,
      Map<byte[], List<KeyValue>> familyMap) throws IOException {
    byte[] row = null;
    long timestamp = -1;
    Document doc = new Document();
    boolean added = false;
    for (Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
      String family = Bytes.toString(entry.getKey());
      for (KeyValue kv : entry.getValue()) {
        //System.out.println("key:"+kv.getKeyString());
        if (row == null) {
          row = kv.getRow();
          String rowStr = Bytes.toString(row);
          //System.out.println("rowstr:" + rowStr);
          timestamp = kv.getTimestamp();
        }
        // the row should always be the same
        assert Arrays.equals(row, kv.getRow());
        
        String name = Bytes.toStringBinary(kv.getQualifier());
        String value = Bytes.toStringBinary(kv.getValue());
        Field field = new Field(name, value, Store.NO, Index.ANALYZED);
        doc.add(field);
        added = true;
      }
    }
    if (!added) {
      return null;
    }
    addFields(row, timestamp, doc, lucene);
    return doc;
  }

  private void addFields(byte[] row, long timestamp, Document doc,
      LuceneCoprocessor lucene) {
    String rowStr = new String(row);
    Field rowField = new Field(LuceneCoprocessor.ROW_FIELD, rowStr, Store.NO,
        Index.NOT_ANALYZED);
    doc.add(rowField);
    String uid = lucene.toUID(rowStr, timestamp);
    Field uidField = new Field(LuceneCoprocessor.UID_FIELD, uid, Store.YES,
        Index.NOT_ANALYZED);
    doc.add(uidField);
  }
}
