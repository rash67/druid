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
import org.apache.druid.java.util.common.IAE;

public enum BufferEncoding
{
  RAW_DOUBLES,
  UPDATE_SKETCH,
  SKETCH,
  UNION;

  public byte toByte()
  {
    int ordinal = ordinal();

    Preconditions.checkState(ordinal <= 255);

    return (byte) ((0xff) & ordinal);
  }

  public static BufferEncoding fromOrdinal(int value)
  {
    switch (value) {
      case 0:
        return BufferEncoding.RAW_DOUBLES;
      case 1:
        return BufferEncoding.UPDATE_SKETCH;
      case 2:
        return BufferEncoding.SKETCH;
      case 3:
        return BufferEncoding.UNION;
      default:
        throw new IAE("invalid %s encoding %s", MetaDoublesSketch.class.getName(), value);
    }
  }
}
