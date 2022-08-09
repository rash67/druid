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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public interface StagedSerde<T>
{
  /**
   * Useful method when some computation is necessary to prepare for serialization without actually writing out
   * all the bytes in order to determine the serialized size. It allows encapsulation of the size computation and
   * the final logical to actually store into a ByteBuffer. It also allows for callers to pack multiple serialized
   * objects into a single ByteBuffer without extra copies of a byte[]/ByteBuffer by using the {@link StorableBuffer}
   * instance returned
   *
   * @param value - object to serialize
   * @return an object that reports its serialized size and how to serialize the object to a ByteBuffer
   */
  StorableBuffer serializeDelayed(@Nullable T value);

  /**
   * Default implementation for when a byte[] is desired. Typically, this default should suffice. Implementing
   * serializeDelayed() includes the logic of how to store into a ByteBuffer
   *
   * @param value - object to serialize
   * @return serialized byte[] of value
   */
  default byte[] serialize(T value)
  {
    StorableBuffer storableBuffer = serializeDelayed(value);
    ByteBuffer byteBuffer = ByteBuffer.allocate(storableBuffer.getSerializedSize()).order(ByteOrder.nativeOrder());

    storableBuffer.store(byteBuffer);

    return byteBuffer.array();
  }

  @Nullable
  T deserialize(ByteBuffer byteBuffer);

  default T deserialize(byte[] bytes)
  {
    return deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
  }
}
