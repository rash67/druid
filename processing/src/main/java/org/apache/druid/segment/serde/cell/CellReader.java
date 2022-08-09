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

package org.apache.druid.segment.serde.cell;

import org.apache.druid.segment.data.CompressionStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CellReader
{
  private final CellIndexReader cellIndexReader;
  private final BlockCompressedPayloadReader dataReader;

  private CellReader(CellIndexReader cellIndexReader, BlockCompressedPayloadReader dataReader)
  {
    this.cellIndexReader = cellIndexReader;
    this.dataReader = dataReader;
  }

  public ByteBuffer getCell(int rowNumber)
  {
    PayloadEntrySpan payloadEntrySpan = cellIndexReader.getEntrySpan(rowNumber);
    ByteBuffer payload = dataReader.read(payloadEntrySpan.getStart(), payloadEntrySpan.getSize());

    return payload;
  }

  public static class Builder
  {
    private final ByteBuffer cellIndexBuffer;
    private final ByteBuffer dataStorageBuffer;

    private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

    // this parses the buffer once into the index and data portions
    public Builder(ByteBuffer originalByteBuffer)
    {
      ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

      int cellIndexSize = masterByteBuffer.getInt();
      cellIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      cellIndexBuffer.limit(cellIndexBuffer.position() + cellIndexSize);

      masterByteBuffer.position(masterByteBuffer.position() + cellIndexSize);

      int dataStorageSize = masterByteBuffer.getInt();
      dataStorageBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      dataStorageBuffer.limit(dataStorageBuffer.position() + dataStorageSize);
    }

    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      this.compressionStrategy = compressionStrategy;

      return this;
    }

    // this creates read only copies of the parsed ByteBuffers along with buffers for decompressing blocks into
    public CellReader build(ByteBuffer cellIndexUncompressedByteBuffer, ByteBuffer dataUncompressedByteBuffer)
    {
      CellIndexReader cellIndexReader = new CellIndexReader(BlockCompressedPayloadReader.create(
          cellIndexBuffer,
          cellIndexUncompressedByteBuffer,
          compressionStrategy.getDecompressor()
      ));
      BlockCompressedPayloadReader dataReader = BlockCompressedPayloadReader.create(
          dataStorageBuffer,
          dataUncompressedByteBuffer,
          compressionStrategy.getDecompressor()
      );
      CellReader cellReader = new CellReader(cellIndexReader, dataReader);

      return cellReader;
    }
  }
}
