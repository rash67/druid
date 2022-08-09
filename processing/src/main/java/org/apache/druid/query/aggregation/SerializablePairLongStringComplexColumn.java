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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.serde.cell.CellReader;
import org.apache.druid.segment.serde.cell.NativeClearedByteBufferProvider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SerializablePairLongStringComplexColumn implements ComplexColumn
{
  private final Closer closer;
  private final int serializedSize;
  private final CellReader cellReader;
  private final SerializablePairLongStringDeltaEncodedStagedSerde serde;

  public SerializablePairLongStringComplexColumn(
      CellReader cellReader,
      SerializablePairLongStringDeltaEncodedStagedSerde serde,
      Closer closer,
      int serializedSize
  )
  {
    this.cellReader = cellReader;
    this.serde = serde;
    this.closer = closer;
    this.serializedSize = serializedSize;
  }

  @Override
  public Class<?> getClazz()
  {
    return SerializablePairLongString.class;
  }

  @Override
  public String getTypeName()
  {
    return SerializablePairLongStringComplexMetricSerde.TYPE_NAME;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public Object getRowValue(int rowNum)
  {
    // nulls are handled properly by the aggregator
    return serde.deserialize(cellReader.getCell(rowNum));
  }

  @Override
  public int getLength()
  {
    return serializedSize;
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RE(e, "error closing " + getClass().getName());
    }
  }

  public static class Factory
  {
    private final int serializedSize;
    private final NativeClearedByteBufferProvider byteBufferProvider;
    private final SerializablePairLongStringDeltaEncodedStagedSerde serde;
    private final CellReader.Builder cellReaderBuilder;

    public Factory(ByteBuffer buffer, NativeClearedByteBufferProvider byteBufferProvider)
    {
      this.byteBufferProvider = byteBufferProvider;

      ByteBuffer masterByteBuffer = buffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

      serializedSize = masterByteBuffer.remaining();

      SerializablePairLongStringColumnHeader columnHeader =
          SerializablePairLongStringColumnHeader.fromBuffer(masterByteBuffer);

      Preconditions.checkArgument(
          columnHeader.getVersion() == SerializablePairLongStringComplexMetricSerde.EXPECTED_VERSION,
          "version %s expected, got %s",
          SerializablePairLongStringComplexMetricSerde.EXPECTED_VERSION,
          columnHeader.getVersion()
      );
      serde = columnHeader.createSerde();
      cellReaderBuilder = new CellReader.Builder(masterByteBuffer);
    }

    public SerializablePairLongStringComplexColumn create()
    {
      Closer closer = Closer.create();
      ResourceHolder<ByteBuffer> cellIndexUncompressedBlockHolder = byteBufferProvider.get();
      ResourceHolder<ByteBuffer> dataUncompressedBlockHolder = byteBufferProvider.get();

      closer.register(cellIndexUncompressedBlockHolder);
      closer.register(dataUncompressedBlockHolder);

      CellReader cellReader =
          cellReaderBuilder.build(cellIndexUncompressedBlockHolder.get(), dataUncompressedBlockHolder.get());

      return new SerializablePairLongStringComplexColumn(cellReader, serde, closer, serializedSize);
    }
  }
}
