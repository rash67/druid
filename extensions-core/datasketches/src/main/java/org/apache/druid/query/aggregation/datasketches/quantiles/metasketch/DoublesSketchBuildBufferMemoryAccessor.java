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

import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchBuildBufferAggregatorHelper;

import java.nio.ByteBuffer;

public class DoublesSketchBuildBufferMemoryAccessor implements MemoryAccessor<UpdateDoublesSketch>
{
  private final DoublesSketchBuildBufferAggregatorHelper helper;

  public DoublesSketchBuildBufferMemoryAccessor(DoublesSketchBuildBufferAggregatorHelper helper)
  {
    this.helper = helper;
  }

  @Override
  public void init(ByteBuffer byteBuffer, int position)
  {
    helper.init(byteBuffer, position);
  }

  @Override
  public UpdateDoublesSketch wrap(ByteBuffer byteBuffer, int position)
  {
    UpdateDoublesSketch sketchAtPosition = helper.getSketchAtPosition(
        byteBuffer,
        position
    );

    return sketchAtPosition;
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }

  @Override
  public void clear()
  {
    helper.clear();
  }
}
