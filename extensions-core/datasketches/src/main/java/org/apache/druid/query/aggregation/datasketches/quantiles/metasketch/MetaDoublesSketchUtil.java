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

import com.google.common.base.Supplier;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;

import java.nio.ByteBuffer;


public class MetaDoublesSketchUtil
{
  public static <T> Supplier<T> UoeSupplier(Class<T> clazz, String type)
  {
    return () -> {
      throw new UOE("unable to construct %s", type);
    };
  }

  public static Supplier<DoublesUnion> UnionUoeSupplier()
  {
    return UoeSupplier(DoublesUnion.class, "union");
  }

  public static Supplier<UpdateDoublesSketch> SketchUoeSupplier()
  {
    return UoeSupplier(UpdateDoublesSketch.class, "sketch");
  }

  public static <T> MemoryAccessor<T> UeoMemoryAccessor(Class<T> clazz, String type)
  {
    return new MemoryAccessor<T>()
    {
      @Override
      public void init(ByteBuffer byteBuffer, int position)
      {
        throw new UOE("unable to construct %s", type);
      }

      @Override
      public T wrap(ByteBuffer byteBuffer, int position)
      {
        throw new UOE("unable to construct %s", type);
      }

      @Override
      public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
      {
        throw new UOE("unable to construct %s", type);
      }

      @Override
      public void clear()
      {
        throw new UOE("unable to construct %s", type);
      }
    };
  }

  public static MemoryAccessor<UpdateDoublesSketch> UpdateSketchUoeMemoryAccessor()
  {
    return UeoMemoryAccessor(UpdateDoublesSketch.class, "updateSketch");
  }

  public static MemoryAccessor<DoublesSketch> SketchUoeByteMemoryWrapper()
  {
    return UeoMemoryAccessor(DoublesSketch.class, "sketch");
  }

  public static MetaDoublesSketch deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof byte[]) {
      return deserialize((byte[]) serializedSketch);
    } else if (serializedSketch instanceof String) {
      return deserialize((String) serializedSketch);
    } else {
      throw new IAE("unexpected type for MetaDoublesSketch.deserialize(): %s", serializedSketch.getClass().getName());
    }
  }

  public static MetaDoublesSketch deserialize(String utf8String)
  {
    return deserialize(StringUtils.decodeBase64String(utf8String));
  }

  public static MetaDoublesSketch deserialize(byte[] bytes)
  {
    return MetaDoublesSketch.fromWireBytes(bytes);
  }
}