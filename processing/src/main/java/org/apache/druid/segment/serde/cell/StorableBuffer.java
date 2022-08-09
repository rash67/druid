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

import java.nio.ByteBuffer;

/**
 * useful when work needs to be done to prepare for serialization (eg encoding) which is also necessary
 * to know how large a buffer is needed. Hence, returns both the size and a method to store in a buffer
 * caller must allocate of sufficient size
 */
public interface StorableBuffer
{
  StorableBuffer EMPTY = new StorableBuffer()
  {
    @Override
    public void store(ByteBuffer byteBuffer)
    {
    }

    @Override
    public int getSerializedSize()
    {
      return 0;
    }
  };

  void store(ByteBuffer byteBuffer);

  int getSerializedSize();
}
