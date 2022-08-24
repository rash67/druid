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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.DoublesUnionMergeBufferMemoryAccessor;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.MetaDoublesSketch;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.MetaDoublesUnion;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

public class DoublesSketchMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<DoublesSketch> selector;
  private final DoublesSketchMergeBufferAggregatorHelper helper;
  private final DoublesUnionMergeBufferMemoryAccessor doublesUnionAccessor;

  public DoublesSketchMergeBufferAggregator(
      final ColumnValueSelector<DoublesSketch> selector,
      final int k,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.helper = new DoublesSketchMergeBufferAggregatorHelper(
        k,
        maxIntermediateSize - MetaDoublesSketch.MEMORY_BUFFER_HEADER_SIZE
    );
    doublesUnionAccessor = new DoublesUnionMergeBufferMemoryAccessor(helper);
  }

  @Override
  public void init(final ByteBuffer buffer, final int position)
  {
    MetaDoublesUnion.initNewBuffer(buffer, position);
  }

  @Override
  public void aggregate(final ByteBuffer buffer, final int position)
  {
    DoublesSketchMergeAggregator.updateMetaUnion(
        selector,
        MetaDoublesUnion.wrapMemoryBuffer(buffer, position, doublesUnionAccessor)
    );
  }

  @Override
  public Object get(final ByteBuffer buffer, final int position)
  {
    return helper.getSketchAtPosition(buffer, position).getResult();
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void close()
  {
    helper.clear();
  }

  // A small number of sketches may run out of the given memory, request more memory on heap and move there.
  // In that case we need to reuse the object from the cache as opposed to wrapping the new buffer.
  @Override
  public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    // TODO
    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
