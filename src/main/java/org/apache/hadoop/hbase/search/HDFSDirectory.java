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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * An HDFS based Lucene Directory implementation.
 * 
 * MMap'ing the block file is used for efficiency, however it's recommended only
 * for 64-bit Unix OS systems
 */
public class HDFSDirectory extends Directory {
  private static final Log LOG = LogFactory.getLog(HDFSDirectory.class);
  private FileSystem fileSystem;
  private String rootPath;

  public HDFSDirectory(FileSystem fileSystem, String path) throws IOException {
    this.fileSystem = fileSystem;
    this.rootPath = path;
    setLockFactory(new HDFSLockFactory(path, fileSystem));
  }

  @Override
  public IndexOutput createOutput(String name) throws IOException {
    return new HDFSIndexOutput(getPath(name));
  }

  protected class HDFSIndexOutput extends BufferedIndexOutput {
    private Path path;
    private FSDataOutputStream output;

    public HDFSIndexOutput(Path path) throws IOException {
      this.path = path;
      short replication = fileSystem.getDefaultReplication();
      Configuration conf = fileSystem.getConf();
      int bufferSize = conf.getInt("io.file.buffer.size", 4096);
      long blockSize = (long) 32 * 1024 * 1024 * 1024;
      output = fileSystem.create(path, false, bufferSize, replication,
          blockSize);
    }

    @Override
    protected void flushBuffer(byte[] b, int offset, int len)
        throws IOException {
      output.write(b, offset, len);
    }

    @Override
    public void flush() throws IOException {
      super.flush();
      output.flush();
    }

    @Override
    public void close() throws IOException {
      flush();
      output.close();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long length() throws IOException {
      return output.getPos();
    }

    @Override
    public void setLength(long length) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
  }

  @Override
  public String[] listAll() throws IOException {
    List<String> files = new ArrayList<String>();
    FileStatus[] statuses = fileSystem.listStatus(new Path(rootPath));
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }
    return (String[]) files.toArray(new String[0]);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
  }

  @Override
  public long fileLength(String name) throws IOException {
    return fileSystem.getFileStatus(new Path(rootPath, name)).getLen();
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return fileSystem.exists(new Path(rootPath, name));
  }

  @Override
  public long fileModified(String name) throws IOException {
    return fileSystem.getFileStatus(new Path(rootPath, name))
        .getModificationTime();
  }

  @Override
  public void touchFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void deleteFile(String name) throws IOException {
    Path path = getPath(name);
    boolean deleted = fileSystem.delete(path, false);
  }

  private Path getPath(String name) {
    return new Path(rootPath + "/" + name);
  }

  @Override
  public IndexInput openInput(String name) throws IOException {
    return openInput(name, BufferedIndexInput.BUFFER_SIZE);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    Path path = getPath(name);
    // open the HDFS input to obtain the
    // underlying block file
    final FSDataInputStream dataInput = fileSystem.open(path, bufferSize);
    final DFSClient.DFSInputStream dfsInput = (DFSClient.DFSInputStream) dataInput
        .getInput();
    try {
      File file = dfsInput.getFile();
      if (file == null) {
        throw new IOException("file is null");
      }
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      try {
        return new HDFSMMapIndexInput(raf);
      } finally {
        raf.close();
      }
    } finally {
      dfsInput.close();
    }
  }

  private class HDFSMMapIndexInput extends IndexInput {
    private ByteBuffer buffer;
    private final long length;
    private boolean isClone = false;

    private HDFSMMapIndexInput(RandomAccessFile raf) throws IOException {
      this.length = raf.length();
      this.buffer = raf.getChannel().map(MapMode.READ_ONLY, 0, length);
    }

    @Override
    public byte readByte() throws IOException {
      try {
        return buffer.get();
      } catch (BufferUnderflowException e) {
        throw new IOException("read past EOF");
      }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      try {
        buffer.get(b, offset, len);
      } catch (BufferUnderflowException e) {
        throw new IOException("read past EOF");
      }
    }

    @Override
    public short readShort() throws IOException {
      try {
        return buffer.getShort();
      } catch (BufferUnderflowException e) {
        throw new IOException("read past EOF");
      }
    }

    @Override
    public int readInt() throws IOException {
      try {
        return buffer.getInt();
      } catch (BufferUnderflowException e) {
        throw new IOException("read past EOF");
      }
    }

    @Override
    public long readLong() throws IOException {
      try {
        return buffer.getLong();
      } catch (BufferUnderflowException e) {
        throw new IOException("read past EOF");
      }
    }

    @Override
    public long getFilePointer() {
      return buffer.position();
    }

    @Override
    public void seek(long pos) throws IOException {
      buffer.position((int) pos);
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public Object clone() {
      if (buffer == null)
        throw new AlreadyClosedException("MMapIndexInput already closed");
      HDFSMMapIndexInput clone = (HDFSMMapIndexInput) super.clone();
      clone.isClone = true;
      clone.buffer = buffer.duplicate();
      return clone;
    }

    @Override
    public void close() throws IOException {
      // unmap the buffer (if enabled) and at least unset it for GC
      try {
        if (isClone || buffer == null)
          return;
        cleanMapping(buffer);
      } finally {
        buffer = null;
      }
    }
  }

  final void cleanMapping(final ByteBuffer buffer) throws IOException {
    if (MMapDirectory.UNMAP_SUPPORTED) {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
          public Object run() throws Exception {
            final Method getCleanerMethod = buffer.getClass().getMethod(
                "cleaner");
            getCleanerMethod.setAccessible(true);
            final Object cleaner = getCleanerMethod.invoke(buffer);
            if (cleaner != null) {
              cleaner.getClass().getMethod("clean").invoke(cleaner);
            }
            return null;
          }
        });
      } catch (PrivilegedActionException e) {
        final IOException ioe = new IOException(
            "unable to unmap the mapped buffer");
        ioe.initCause(e.getCause());
        throw ioe;
      }
    }
  }
}
