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

package org.apache.druid.storage.google;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GoogleStorageTest
{
  Storage mockStorage;
  GoogleStorage googleStorage;

  Blob blob;

  static final String BUCKET = "bucket";
  static final String PATH = "/path";
  static final long SIZE = 100;
  static final OffsetDateTime UPDATE_TIME = OffsetDateTime.MIN;
  private static final Exception STORAGE_EXCEPTION = new StorageException(404, "Runtime Storage Exception");


  @Before
  public void setUp()
  {
    mockStorage = EasyMock.mock(Storage.class);

    googleStorage = new GoogleStorage(() -> mockStorage);

    blob = EasyMock.mock(Blob.class);
  }

  @Test
  public void testInsertDefaultBufferSize() throws IOException
  {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    final Capture<InputStream> inputStreamCapture = Capture.newInstance();
    final AbstractInputStreamContent httpContent = EasyMock.createMock(AbstractInputStreamContent.class);
    EasyMock.expect(httpContent.getInputStream()).andReturn(inputStream);
    EasyMock.expect(
        mockStorage.createFrom(
            EasyMock.eq(BlobInfo.newBuilder(BlobId.of(BUCKET, PATH)).build()),
            EasyMock.capture(inputStreamCapture)
        )
    ).andReturn(blob);
    EasyMock.replay(httpContent, mockStorage, blob);
    googleStorage.insert(BUCKET, PATH, httpContent, null);
    EasyMock.verify(httpContent, mockStorage, blob);
  }

  @Test
  public void testInsertCustomBufferSize() throws IOException
  {
    final int bufferSize = 100;
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    final Capture<InputStream> inputStreamCapture = Capture.newInstance();
    final AbstractInputStreamContent httpContent = EasyMock.createMock(AbstractInputStreamContent.class);
    EasyMock.expect(httpContent.getInputStream()).andReturn(inputStream);
    EasyMock.expect(
        mockStorage.createFrom(
            EasyMock.eq(BlobInfo.newBuilder(BlobId.of(BUCKET, PATH)).build()),
            EasyMock.capture(inputStreamCapture),
            EasyMock.eq(bufferSize)
        )
    ).andReturn(blob);
    EasyMock.replay(httpContent, mockStorage, blob);
    googleStorage.insert(BUCKET, PATH, httpContent, bufferSize);
    EasyMock.verify(httpContent, mockStorage, blob);
  }

  @Test
  public void testDeleteSuccess()
  {
    EasyMock.expect(mockStorage.delete(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andReturn(true);
    EasyMock.replay(mockStorage);
    googleStorage.delete(BUCKET, PATH);
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testDeleteFileNotFound()
  {
    EasyMock.expect(mockStorage.delete(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andReturn(false);
    EasyMock.replay(mockStorage);
    googleStorage.delete(BUCKET, PATH);
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testDeleteFailure()
  {
    EasyMock.expect(mockStorage.delete(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andThrow(STORAGE_EXCEPTION);
    EasyMock.replay(mockStorage);
    Assert.assertThrows(StorageException.class, () -> googleStorage.delete(BUCKET, PATH));
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testBatchDeleteSuccess()
  {
    List<String> paths = ImmutableList.of("/path1", "/path2");
    final Capture<Iterable<BlobId>> pathIterable = Capture.newInstance();
    EasyMock.expect(mockStorage.delete(EasyMock.capture(pathIterable))).andReturn(ImmutableList.of(true, true));
    EasyMock.replay(mockStorage);

    googleStorage.batchDelete(BUCKET, paths);

    List<BlobId> recordedBlobIds = new ArrayList<>();
    pathIterable.getValue().iterator().forEachRemaining(recordedBlobIds::add);

    List<String> recordedPaths = recordedBlobIds.stream().map(BlobId::getName).collect(Collectors.toList());

    assertTrue(paths.size() == recordedPaths.size() && paths.containsAll(recordedPaths) && recordedPaths.containsAll(
        paths));
    assertEquals(BUCKET, recordedBlobIds.get(0).getBucket());
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testBatchDeleteFileNotFound()
  {
    List<String> paths = ImmutableList.of("/path1", "/path2");
    final Capture<Iterable<BlobId>> pathIterable = Capture.newInstance();
    EasyMock.expect(mockStorage.delete(EasyMock.capture(pathIterable))).andReturn(ImmutableList.of(true, false));
    EasyMock.replay(mockStorage);

    googleStorage.batchDelete(BUCKET, paths);

    List<BlobId> recordedBlobIds = new ArrayList<>();
    pathIterable.getValue().iterator().forEachRemaining(recordedBlobIds::add);

    List<String> recordedPaths = recordedBlobIds.stream().map(BlobId::getName).collect(Collectors.toList());

    assertTrue(paths.size() == recordedPaths.size());
    assertTrue(paths.containsAll(recordedPaths));
    assertTrue(recordedPaths.containsAll(paths));
    assertEquals(BUCKET, recordedBlobIds.get(0).getBucket());
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testBatchDeleteFailure()
  {
    List<String> paths = ImmutableList.of("/path1", "/path2");
    EasyMock.expect(mockStorage.delete((Iterable<BlobId>) EasyMock.anyObject()))
            .andThrow(STORAGE_EXCEPTION);
    EasyMock.replay(mockStorage);
    Assert.assertThrows(StorageException.class, () -> googleStorage.batchDelete(BUCKET, paths));
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testGetMetadataMatch() throws IOException
  {
    EasyMock.expect(mockStorage.get(
        EasyMock.eq(BUCKET),
        EasyMock.eq(PATH),
        EasyMock.anyObject(Storage.BlobGetOption.class)
    )).andReturn(blob);

    EasyMock.expect(blob.getBucket()).andReturn(BUCKET);
    EasyMock.expect(blob.getName()).andReturn(PATH);
    EasyMock.expect(blob.getSize()).andReturn(SIZE);
    EasyMock.expect(blob.getUpdateTimeOffsetDateTime()).andReturn(UPDATE_TIME);

    EasyMock.replay(mockStorage, blob);

    GoogleStorageObjectMetadata objectMetadata = googleStorage.getMetadata(BUCKET, PATH);
    assertEquals(
        objectMetadata,
        new GoogleStorageObjectMetadata(BUCKET, PATH, SIZE, UPDATE_TIME.toEpochSecond() * 1000)
    );

    EasyMock.verify(mockStorage);
  }

  @Test
  public void testExistsTrue()
  {
    EasyMock.expect(mockStorage.get(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andReturn(blob);
    EasyMock.replay(mockStorage);
    assertTrue(googleStorage.exists(BUCKET, PATH));
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testExistsFalse()
  {
    EasyMock.expect(mockStorage.get(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andReturn(null);
    EasyMock.replay(mockStorage);
    assertFalse(googleStorage.exists(BUCKET, PATH));
    EasyMock.verify(mockStorage);
  }

  @Test
  public void testSize() throws IOException
  {
    EasyMock.expect(mockStorage.get(
        EasyMock.eq(BUCKET),
        EasyMock.eq(PATH),
        EasyMock.anyObject(Storage.BlobGetOption.class)
    )).andReturn(blob);

    EasyMock.expect(blob.getSize()).andReturn(SIZE);

    EasyMock.replay(mockStorage, blob);

    long size = googleStorage.size(BUCKET, PATH);

    assertEquals(size, SIZE);
    EasyMock.verify(mockStorage, blob);
  }

  @Test
  public void testVersion() throws IOException
  {
    final String etag = "abcd";
    EasyMock.expect(mockStorage.get(
        EasyMock.eq(BUCKET),
        EasyMock.eq(PATH),
        EasyMock.anyObject(Storage.BlobGetOption.class)
    )).andReturn(blob);

    EasyMock.expect(blob.getEtag()).andReturn(etag);

    EasyMock.replay(mockStorage, blob);

    assertEquals(etag, googleStorage.version(BUCKET, PATH));
    EasyMock.verify(mockStorage, blob);
  }

  @Test
  public void testList() throws IOException
  {
    Page<Blob> blobPage = EasyMock.mock(Page.class);
    EasyMock.expect(mockStorage.list(
        EasyMock.eq(BUCKET),
        EasyMock.anyObject()
    )).andReturn(blobPage);

    Blob blob1 = EasyMock.mock(Blob.class);
    Blob blob2 = EasyMock.mock(Blob.class);

    final String bucket1 = "BUCKET_1";
    final String path1 = "PATH_1";
    final long size1 = 7;
    final OffsetDateTime updateTime1 = OffsetDateTime.MIN;

    final String bucket2 = "BUCKET_2";
    final String path2 = "PATH_2";
    final long size2 = 9;
    final OffsetDateTime updateTime2 = OffsetDateTime.MIN;

    final String nextPageToken = "TOKEN";

    EasyMock.expect(blob1.getBucket()).andReturn(bucket1);
    EasyMock.expect(blob1.getName()).andReturn(path1);
    EasyMock.expect(blob1.getSize()).andReturn(size1);
    EasyMock.expect(blob1.getUpdateTimeOffsetDateTime()).andReturn(updateTime1);

    EasyMock.expect(blob2.getBucket()).andReturn(bucket2);
    EasyMock.expect(blob2.getName()).andReturn(path2);
    EasyMock.expect(blob2.getSize()).andReturn(size2);
    EasyMock.expect(blob2.getUpdateTimeOffsetDateTime()).andReturn(updateTime2);


    List<Blob> blobs = ImmutableList.of(blob1, blob2);

    EasyMock.expect(blobPage.streamValues()).andReturn(blobs.stream());

    EasyMock.expect(blobPage.getNextPageToken()).andReturn(nextPageToken);


    EasyMock.replay(mockStorage, blobPage, blob1, blob2);

    GoogleStorageObjectMetadata objectMetadata1 = new GoogleStorageObjectMetadata(
        bucket1,
        path1,
        size1,
        updateTime1.toEpochSecond() * 1000
    );
    GoogleStorageObjectMetadata objectMetadata2 = new GoogleStorageObjectMetadata(
        bucket2,
        path2,
        size2,
        updateTime2.toEpochSecond() * 1000
    );

    GoogleStorageObjectPage objectPage = googleStorage.list(BUCKET, PATH, null, null);

    assertEquals(objectPage.getObjectList().get(0), objectMetadata1);
    assertEquals(objectPage.getObjectList().get(1), objectMetadata2);
    assertEquals(objectPage.getNextPageToken(), nextPageToken);

    EasyMock.verify(mockStorage, blobPage, blob1, blob2);
  }
}
