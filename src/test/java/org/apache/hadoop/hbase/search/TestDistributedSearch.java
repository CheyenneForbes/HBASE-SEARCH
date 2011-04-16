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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.search.LuceneProtocol.ScoreUID;
import org.apache.hadoop.hbase.util.Bytes;

public class TestDistributedSearch extends TestCase {
  /**
   * Test the writable serialization.
   */
  public void testWritables() throws Exception {
    ScoreUID[] uids = new ScoreUID[10];
    for (int x = 0; x < uids.length; x++) {
      uids[x] = new ScoreUID((float) x, Bytes.toBytes(x),
          System.currentTimeMillis());
    }
    LuceneProtocol.Results results = new LuceneProtocol.Results(25, uids);
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    results.write(dataOut);

    byte[] bytes = byteOut.toByteArray();
    DataInputStream dataIn = new DataInputStream(
        new ByteArrayInputStream(bytes));
    LuceneProtocol.Results inResults = new LuceneProtocol.Results();
    inResults.readFields(dataIn);

    for (int x = 0; x < inResults.uids.length; x++) {
      assert inResults.uids[x].equals(uids[x]);
    }
  }
}
