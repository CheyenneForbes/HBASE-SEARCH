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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.Writable;

public interface LuceneProtocol extends CoprocessorProtocol {
  /**
   * Delete rows from HBase that match the given query string.
   */
  public void delete(String queryString) throws Exception;

  public Results search(String queryString, int hits) throws Exception;
  
  /**
   * Results to be returned from a search call.
   */
  public static class Results implements Writable {
    public int numFound;
    public ScoreUID[] uids;

    public Results() {
    }

    public Results(int numFound, ScoreUID[] uids) {
      this.numFound = numFound;
      this.uids = uids;
    }

    @Override
    public String toString() {
      return "Results [numFound=" + numFound + ", uids="
          + Arrays.toString(uids) + "]";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Results other = (Results) obj;
      if (numFound != other.numFound)
        return false;
      if (!Arrays.equals(uids, other.uids))
        return false;
      return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(numFound);
      if (uids == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeInt(uids.length);
        for (int x = 0; x < uids.length; x++) {
          if (uids[x] != null) {
            out.writeBoolean(true);
            uids[x].write(out);
          } else {
            out.writeBoolean(false);
          }
        }
      }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      numFound = in.readInt();
      boolean val = in.readBoolean();
      if (val) {
        int len = in.readInt();
        uids = new ScoreUID[len];
        for (int x = 0; x < uids.length; x++) {
          boolean n = in.readBoolean();
          if (n) {
            uids[x] = new ScoreUID();
            uids[x].readFields(in);
          }
        }
      }
    }
  }
  
  /**
   * Contains the score for a given row and timestamp.
   */
  public static class ScoreUID implements Writable {
    public float score;
    public byte[] row;
    public long timestamp;
    
    public ScoreUID() {}
    
    public ScoreUID(float score, byte[] row, long timestamp) {
      this.score = score;
      this.row = row;
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ScoreUID other = (ScoreUID) obj;
      if (!Arrays.equals(row, other.row))
        return false;
      if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
        return false;
      if (timestamp != other.timestamp)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "ScoreUID [score=" + score + ", row=" + Arrays.toString(row)
          + ", timestamp=" + timestamp + "]";
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeFloat(score);
      out.writeInt(row.length);
      out.write(row);
      out.writeLong(timestamp);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      score = in.readFloat();
      int len = in.readInt();
      row = new byte[len];
      in.readFully(row);
      timestamp = in.readLong();
    }
  }
}