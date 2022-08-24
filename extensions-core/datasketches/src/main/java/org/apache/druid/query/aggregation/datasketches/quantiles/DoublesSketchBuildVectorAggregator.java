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

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.DoublesSketchBuildBufferMemoryAccessor;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.MetaDoublesSketch;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DoublesSketchBuildVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;
  private final DoublesSketchBuildBufferMemoryAccessor updateSketchAccessor;
  private final DoublesSketchBuildBufferAggregatorHelper helper;

  DoublesSketchBuildVectorAggregator(
      final VectorValueSelector selector,
      final int size,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.helper = new DoublesSketchBuildBufferAggregatorHelper(
        size,
        maxIntermediateSize - MetaDoublesSketch.MEMORY_BUFFER_HEADER_SIZE
    );
    updateSketchAccessor = new DoublesSketchBuildBufferMemoryAccessor(helper);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    MetaDoublesSketch.initNewBuffer(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final double[] doubles = selector.getDoubleVector();
    final boolean[] nulls = selector.getNullVector();
    MetaDoublesSketch metaDoublesSketch = MetaDoublesSketch.wrapMemoryBuffer(
        buf,
        position,
        new DoublesSketchBuildBufferMemoryAccessor(helper)
    );

    for (int i = startRow; i < endRow; i++) {
      if (nulls == null || !nulls[i]) {
        metaDoublesSketch.update(doubles[i]);
      }
    }
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    final double[] doubles = selector.getDoubleVector();
    final boolean[] nulls = selector.getNullVector();

    for (int i = 0; i < numRows; i++) {
      final int idx = rows != null ? rows[i] : i;

      if (nulls == null || !nulls[idx]) {
        final int position = positions[i] + positionOffset;
        MetaDoublesSketch metaDoublesSketch = MetaDoublesSketch.wrapMemoryBuffer(buf, position, updateSketchAccessor);

        metaDoublesSketch.update(doubles[idx]);
      }
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return helper.get(buf, position);
  }

  @Override
  public void relocate(final int oldPosition, final int newPosition, final ByteBuffer oldBuf, final ByteBuffer newBuf)
  {
    // TODO
    updateSketchAccessor.relocate(oldPosition, newPosition, oldBuf, newBuf);
  }

  @Override
  public void close()
  {
    updateSketchAccessor.clear();
  }
}
