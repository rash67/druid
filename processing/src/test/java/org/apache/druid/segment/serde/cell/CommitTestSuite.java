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

import org.apache.druid.query.aggregation.SerializablePairLongStringBufferStoreTest;
import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerdeTest;
import org.apache.druid.query.aggregation.SerializablePairLongStringDeltaEncodedStagedSerdeTest;
import org.apache.druid.query.aggregation.SerializablePairLongStringSimpleStagedSerdeTest;
import org.apache.druid.query.aggregation.first.StringFirstTimeseriesQueryTest;
import org.apache.druid.query.aggregation.last.StringLastTimeseriesQueryTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    StringFirstTimeseriesQueryTest.class,
    StringLastTimeseriesQueryTest.class,
    SerializablePairLongStringBufferStoreTest.class,
    SerializablePairLongStringComplexMetricSerdeTest.class,
    SerializablePairLongStringDeltaEncodedStagedSerdeTest.class,
    SerializablePairLongStringSimpleStagedSerdeTest.class,
    BlockCompressedPayloadWriterTest.class,
    CellWriterTest.class
})
public class CommitTestSuite
{
}
