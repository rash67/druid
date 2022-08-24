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

import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import java.nio.ByteBuffer;

public interface RawDoublesPseudoSketch extends StorableBuffer
{
  void update(double value);

  void checkAndUpdate(double value);

  boolean hasSpace();

  boolean hasSpaceFor(int count);

  void pushInto(MetaDoublesUnion metaDoublesUnion);

  void pushInto(DoublesUnion doublesUnion);

  void pushInto(UpdateDoublesSketch updateDoublesSketch);

  void pushInto(RawDoublesPseudoSketch otherPseudoSketch);

  int getRawDoublesCount();

  RawDoublesPseudoSketch onHeap();

  @Override
  void store(ByteBuffer byteBuffer);

  @Override
  int getSerializedSize();
}
