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

import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;

public class CellWriterTest extends BytesReadWriteTestBase
{
  public CellWriterTest()
  {
    super(
        new CellWriterToBytesWriter.Builder(
            new CellWriter.Builder(NativeClearedByteBufferProvider.INSTANCE, new OnHeapMemorySegmentWriteOutMedium())
        ),
        ByteWriterTestHelper.ValidationFunctionBuilder.ROW_READER_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, TestCaseResult.of(62))
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, TestCaseResult.of(46))
            .setTestCaseValue(BytesReadWriteTest::testNull, TestCaseResult.of(46))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, TestCaseResult.of(4151))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, TestCaseResult.of(1049204))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, TestCaseResult.of(1053277))
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, TestCaseResult.of(1655))
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, TestCaseResult.of(7368))
            .setTestCaseValue(
                BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads,
                TestCaseResult.of(575673)
            )
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, TestCaseResult.of(65750))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, TestCaseResult.of(845))
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, TestCaseResult.of(3126618))

    );
  }
}
