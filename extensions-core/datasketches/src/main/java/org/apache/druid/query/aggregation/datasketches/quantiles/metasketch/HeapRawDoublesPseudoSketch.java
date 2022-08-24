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
import java.util.Arrays;

public class HeapRawDoublesPseudoSketch implements RawDoublesPseudoSketch
{
  private final double[] rawDoubles;

  private int rawDoublesCount = 0;

  public HeapRawDoublesPseudoSketch(int maxRawDoubles)
  {
    rawDoubles = new double[maxRawDoubles];
  }

  public static RawDoublesPseudoSketch newDefaultMaxSizeInstance()
  {
    return new HeapRawDoublesPseudoSketch(MetaDoublesSketch.MAX_RAW_DOUBLES);
  }

  @Override
  public void update(double value)
  {
    rawDoubles[rawDoublesCount++] = value;
  }

  @Override
  public void checkAndUpdate(double value)
  {
    if (hasSpace()) {
      rawDoubles[rawDoublesCount++] = value;
    } else {
      throw new ISE("attempt to add value to full %s [%s elements]", getClass().getName(), getRawDoublesCount());
    }
  }

  @Override
  public boolean hasSpace()
  {
    return rawDoublesCount + 1 < rawDoubles.length;
  }

  @Override
  public boolean hasSpaceFor(int count)
  {
    return rawDoublesCount + count < rawDoubles.length;
  }

  @Override
  public void pushInto(MetaDoublesUnion metaDoublesUnion)
  {
    Arrays.stream(rawDoubles).forEach(metaDoublesUnion::update);
  }

  @Override
  public void pushInto(DoublesUnion doublesUnion)
  {
    Preconditions.checkNotNull(doublesUnion);
    Arrays.stream(rawDoubles).forEach(doublesUnion::update);
  }

  @Override
  public void pushInto(UpdateDoublesSketch updateDoublesSketch)
  {
    Arrays.stream(rawDoubles).forEach(updateDoublesSketch::update);
  }

  @Override
  public void pushInto(RawDoublesPseudoSketch otherPseudoSketch)
  {
    Arrays.stream(rawDoubles).forEach(otherPseudoSketch::checkAndUpdate);
  }

  @Override
  public int getRawDoublesCount()
  {
    return rawDoublesCount;
  }

  @Override
  public RawDoublesPseudoSketch onHeap()
  {
    return this;
  }

  @Override
  public void store(ByteBuffer byteBuffer)
  {
    byteBuffer.putInt(rawDoublesCount);
    Arrays.stream(rawDoubles).forEach(byteBuffer::putDouble);
  }

  @Override
  public int getSerializedSize()
  {
    return Integer.BYTES + Double.BYTES * rawDoublesCount;
  }
}
