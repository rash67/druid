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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class ByteBufferBitSet
{
  private final static int ADDRESS_BITS_PER_WORD = 6;
  private static final long WORD_INDEX_MASK = 0x000000000000004FL;
  private static final long BIT_CLEARING_BASE = 0xFFFFFFFFFFFFFFFFL;

  private final LongBuffer longBuffer;
  private final int size;

  public ByteBufferBitSet(ByteBuffer byteBuffer)
  {
    this.longBuffer = byteBuffer.asLongBuffer();
    size = byteBuffer.remaining() * 8;
  }

  public boolean get(int bitIndex)
  {
    Preconditions.checkArgument(bitIndex > 0 && bitIndex < size);

    int wordIndex = bitIndex >> ADDRESS_BITS_PER_WORD;
    long word = longBuffer.get(wordIndex);

    long wordMask = 1L << (bitIndex & WORD_INDEX_MASK);
    return (word & wordMask) != 0;
  }

  public void set(int bitIndex)
  {
    Preconditions.checkArgument(bitIndex > 0 && bitIndex < size);

    int wordIndex = bitIndex >> ADDRESS_BITS_PER_WORD;
    long word = longBuffer.get(wordIndex);
    long wordBitIndex = bitIndex & WORD_INDEX_MASK;
    long orWord = 1L << wordBitIndex;

    word |= orWord;
    longBuffer.put(wordIndex, word);
  }

  public void clear(int bitIndex)
  {
    Preconditions.checkArgument(bitIndex > 0 && bitIndex < size);

    int wordIndex = bitIndex >> ADDRESS_BITS_PER_WORD;
    long word = longBuffer.get(wordIndex);
    long wordBitIndex = bitIndex & WORD_INDEX_MASK;
    long mask = (BIT_CLEARING_BASE ^ (1L << wordBitIndex));

    word &= mask;
    longBuffer.put(wordIndex, word);
  }

  public static void main(String[] args)
  {
    int size = 1024 * 10;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    ByteBufferBitSet bitSet = new ByteBufferBitSet(byteBuffer);

    for (int i = 0; i < size; i++) {
      bitSet.set(i);
      if (!bitSet.get(i)) {
        throw new AssertionError(i);
      }
      bitSet.clear(i);
      if (bitSet.get(i)) {
        throw new AssertionError(i);
      }
    }
  }
}
