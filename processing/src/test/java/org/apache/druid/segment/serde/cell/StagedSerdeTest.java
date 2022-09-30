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

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StagedSerdeTest
{
  private static final StagedSerde<String> SERDE = new StagedSerde<String>()
  {
    @Override
    public StorableBuffer serializeDelayed(@Nullable String value)
    {
      if (value == null) {
        return StorableBuffer.EMPTY;
      }

      ByteBuffer serialzed = StringUtils.toUtf8ByteBuffer(value);

      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          byteBuffer.put(serialzed);
        }

        @Override
        public int getSerializedSize()
        {
          return serialzed.remaining();
        }
      };
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer)
    {
      return StringUtils.fromUtf8(byteBuffer);
    }
  };

  @Test(expected = NullPointerException.class)
  public void testDeserializeNullHandling()
  {
    Assert.assertNull(SERDE.deserialize((byte[])null));
  }

  @Test
  public void testSerializeNullHandling()
  {
    Assert.assertTrue(SERDE.serialize(null).length == 0);
  }

  @Test
  public void testBasic()
  {
    String value = "a quick brown fox jumped over the lazy brown dogs";

    Assert.assertEquals(value, SERDE.deserialize(SERDE.serialize(value)));
  }
}