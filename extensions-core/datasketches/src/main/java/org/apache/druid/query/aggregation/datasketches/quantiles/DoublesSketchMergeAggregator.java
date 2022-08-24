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

import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.MetaDoublesSketch;
import org.apache.druid.query.aggregation.datasketches.quantiles.metasketch.MetaDoublesUnion;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class DoublesSketchMergeAggregator implements Aggregator
{
  private final ColumnValueSelector<?> selector;
  @Nullable
  private MetaDoublesUnion union;

  public DoublesSketchMergeAggregator(final ColumnValueSelector<?> selector, final int k)
  {
    this.selector = selector;
    union = MetaDoublesUnion.createFromUnion(DoublesUnion.builder().setMaxK(k).build());
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public synchronized void aggregate()
  {
    updateMetaUnion(selector, union);
  }


  @SuppressWarnings("ConstantConditions")
  @Override
  public synchronized Object get()
  {
    return union.toMetaDoublesSketch();
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void close()
  {
    union = null;
  }

  static void updateMetaUnion(ColumnValueSelector<?> selector, MetaDoublesUnion union)
  {
    final Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (object instanceof MetaDoublesSketch) {
      ((MetaDoublesSketch) object).pushInto(union);
    } else {
      throw new UOE("removed handling of double in DoublesSketch Merge* aggregators as it appeared unnecessary");
    }
  }
}
