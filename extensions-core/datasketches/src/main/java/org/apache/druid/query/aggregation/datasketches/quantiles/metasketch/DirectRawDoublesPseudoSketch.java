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
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public class DirectRawDoublesPseudoSketch implements RawDoublesPseudoSketch
{
  private final DoubleBuffer doubleBuffer;
  private final int maxRawDoubles;
  // header is [sketchSize:int][count:int]
  private final ByteBuffer countByteBuffer;

  private DirectRawDoublesPseudoSketch(int maxRawDoubles, ByteBuffer originalByteBuffer)
  {
    this.maxRawDoubles = maxRawDoubles;
    countByteBuffer = originalByteBuffer.duplicate().order(originalByteBuffer.order());

    ByteBuffer dataByteBuffer = originalByteBuffer.duplicate().order(originalByteBuffer.order());

    dataByteBuffer.position(originalByteBuffer.position() + Integer.BYTES);
    this.doubleBuffer = dataByteBuffer.asDoubleBuffer();
  }

  /**
   * @param byteBuffer - this mutates both contents and position
   * @return instance wrapping memory at ByteBuffer position() - limit()
   */
  public static DirectRawDoublesPseudoSketch wrapDefaultMaxInstance(ByteBuffer byteBuffer)
  {
    return new DirectRawDoublesPseudoSketch(MetaDoublesSketch.MAX_RAW_DOUBLES, byteBuffer);
  }

  public static void initEmptyDefaultMaxInstanceBuffer(ByteBuffer byteBuffer, int position)
  {
    byteBuffer.putInt(position, 0);
  }

  @Override
  public void update(double value)
  {
    int rawDoublesCount = getRawDoublesCount();

    doubleBuffer.put(rawDoublesCount, value);
    putRawDoublesCount(rawDoublesCount + 1);
  }

  @Override
  public void checkAndUpdate(double value)
  {
    if (hasSpace()) {
      update(value);
    } else {
      throw new ISE("attempt to add value to full %s [%s elements]", getClass().getName(), getRawDoublesCount());
    }
  }

  @Override
  public boolean hasSpace()
  {
    return getRawDoublesCount() + 1 <= maxRawDoubles;
  }

  @Override
  public boolean hasSpaceFor(int count)
  {
    return getRawDoublesCount() + count <= maxRawDoubles;
  }

  @Override
  public void pushInto(MetaDoublesUnion metaDoublesUnion)
  {
    Preconditions.checkNotNull(metaDoublesUnion);

    for (int i = 0; i < getRawDoublesCount(); i++) {
      metaDoublesUnion.update(doubleBuffer.get(i));
    }
  }

  @Override
  public void pushInto(DoublesUnion doublesUnion)
  {
    Preconditions.checkNotNull(doublesUnion);

    for (int i = 0; i < getRawDoublesCount(); i++) {
      doublesUnion.update(doubleBuffer.get(i));
    }
  }

  @Override
  public void pushInto(UpdateDoublesSketch updateDoublesSketch)
  {
    for (int i = 0; i < getRawDoublesCount(); i++) {
      updateDoublesSketch.update(doubleBuffer.get(i));
    }
  }

  @Override
  public void pushInto(RawDoublesPseudoSketch otherPseudoSketch)
  {
    for (int i = 0; i < getRawDoublesCount(); i++) {
      otherPseudoSketch.update(doubleBuffer.get(i));
    }
  }

  @Override
  public int getRawDoublesCount()
  {
    return countByteBuffer.getInt(countByteBuffer.position());
  }

  @Override
  public RawDoublesPseudoSketch onHeap()
  {
    HeapRawDoublesPseudoSketch pseudoSketch = new HeapRawDoublesPseudoSketch(maxRawDoubles);
    int rawDoublesCount = getRawDoublesCount();

    Preconditions.checkState(rawDoublesCount <= maxRawDoubles, "exceeds max %s > %s", rawDoublesCount, maxRawDoubles);

    for (int i = 0; i < rawDoublesCount; i++) {
      pseudoSketch.checkAndUpdate(doubleBuffer.get(i));
    }

    return pseudoSketch;
  }

  @Override
  public void store(ByteBuffer byteBuffer)
  {
    int rawDoublesCount = getRawDoublesCount();
    byteBuffer.putInt(rawDoublesCount);

    for (int i = 0; i < rawDoublesCount; i++) {
      byteBuffer.putDouble(doubleBuffer.get(i));
    }
  }

  @Override
  public int getSerializedSize()
  {
    return Double.BYTES * getRawDoublesCount();
  }

  private void putRawDoublesCount(int value)
  {
    countByteBuffer.putInt(countByteBuffer.position(), value);
  }
}
