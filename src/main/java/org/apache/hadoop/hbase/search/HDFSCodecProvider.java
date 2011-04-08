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

import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.SegmentInfosReader;
import org.apache.lucene.index.codecs.SegmentInfosWriter;
import org.apache.lucene.index.codecs.appending.AppendingCodec;
import org.apache.lucene.index.codecs.appending.AppendingSegmentInfosReader;
import org.apache.lucene.index.codecs.appending.AppendingSegmentInfosWriter;

/**
 * This Codec Provider uses the Appending codec
 * which must be used in conjunction with HDFS.
 */
public class HDFSCodecProvider extends CodecProvider {
  AppendingSegmentInfosReader infosReader = new AppendingSegmentInfosReader();
  AppendingSegmentInfosWriter infosWriter = new AppendingSegmentInfosWriter();
  
  public HDFSCodecProvider() {
    register(new AppendingCodec());
    setDefaultFieldCodec(AppendingCodec.CODEC_NAME);
  }
  
  public SegmentInfosWriter getSegmentInfosWriter() {
    return infosWriter;
  }
  
  public SegmentInfosReader getSegmentInfosReader() {
    return infosReader;
  }
}
