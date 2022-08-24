/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.datasketches.quantiles.metasketch;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import java.nio.ByteBuffer;

/**
 * per-cell header indicating primarily if this is a raw list of doubles vs a DoublesSketch
 */
public class MetaDoublesSketchHeader implements StorableBuffer
{
  public static int CURRENT_VERSION = 0;
  // header size is 4 bytes for word alignment for LZ4 (minmatch) compression
  public static final int HEADER_SIZE_BYTES = 4;

  private static final int VERSION_INDEX = 0;
  private static final int ENCODING_INDEX = 1;

  private final byte[] bytes;

  public MetaDoublesSketchHeader(byte[] bytes)
  {
    this.bytes = bytes;
  }

  public MetaDoublesSketchHeader(int version, Encoding encoding)
  {
    bytes = new byte[HEADER_SIZE_BYTES];
    Preconditions.checkArgument(version <= 255, "version exceeds max of 255");
    bytes[VERSION_INDEX] = (byte) version;
    Preconditions.checkArgument(encoding.ordinal() <= 255, "%s (%s) exceeds max of 255", encoding, encoding.ordinal());
    bytes[ENCODING_INDEX] = (byte) encoding.ordinal();
  }

  public static MetaDoublesSketchHeader fromByteBuffer(ByteBuffer byteBuffer)
  {
    byte[] bytes = new byte[HEADER_SIZE_BYTES];

    byteBuffer.get(bytes);

    return new MetaDoublesSketchHeader(bytes);
  }

  public int getVersion()
  {
    return bytes[VERSION_INDEX] & 0xFF;
  }

  public Encoding getEncoding()
  {
    return encodingFromOrdinal(bytes[ENCODING_INDEX]);
  }

  public MetaDoublesSketchHeader withEncoding(Encoding encoding)
  {
    return new MetaDoublesSketchHeader(getVersion(), encoding);
  }

  @Override
  public void store(ByteBuffer byteBuffer)
  {
    byteBuffer.put(bytes);
  }

  @Override
  public int getSerializedSize()
  {
    return HEADER_SIZE_BYTES;
  }

  private static Encoding encodingFromOrdinal(int value)
  {
    switch (value) {
      case 0:
        return Encoding.RAW_DOUBLES;
      case 1:
        return Encoding.SKETCH;
      default:
        throw new IAE("invalid %s encoding %s", MetaDoublesSketch.class.getName(), value);
    }
  }

  public enum Encoding
  {
    RAW_DOUBLES,
    SKETCH,
  }
}
