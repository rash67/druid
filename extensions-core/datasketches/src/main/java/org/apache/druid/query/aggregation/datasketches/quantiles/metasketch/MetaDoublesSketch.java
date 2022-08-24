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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * this class provides for keeping a raw list of doubles which is smaller than a {@link DoublesSketch} for small
 * lists. In the case that the max list size is exceeded, it contains a supplier that creates an
 * {@link UpdateDoublesSketch} and numbers are purshed into it.
 */
public class MetaDoublesSketch
{
  public static int MAX_RAW_DOUBLES = 5;
  public static int MEMORY_BUFFER_HEADER_SIZE = Byte.BYTES;


  @Nullable
  private RawDoublesPseudoSketch pseudoSketch;
  @Nullable
  private final Supplier<UpdateDoublesSketch> updateSketchSupplier;
  @Nullable
  private final DoublesSketch sketch;
  private final StateUpdateFunction stateUpdateFunction;

  private State state;

  private MetaDoublesSketch(
      @Nullable
      RawDoublesPseudoSketch pseudoSketch,
      Supplier<UpdateDoublesSketch> updateSketchSupplier,
      @Nullable
      DoublesSketch sketch,
      State initialState,
      StateUpdateFunction stateUpdateFunction
  )
  {
    this.pseudoSketch = pseudoSketch;
    this.updateSketchSupplier = Suppliers.memoize(updateSketchSupplier);
    this.sketch = sketch;
    state = initialState;
    this.stateUpdateFunction = stateUpdateFunction;
  }

  public static MetaDoublesSketch createFromRawDoubles(
      RawDoublesPseudoSketch pseudoSketch,
      Supplier<UpdateDoublesSketch> updateSketchSupplier
  )
  {
    return new MetaDoublesSketch(
        pseudoSketch,
        updateSketchSupplier,
        null,
        State.RAW_DOUBLES,
        StateUpdateFunction.NO_OP
    );
  }

  public static MetaDoublesSketch createFromRawDoublesBuffer(
      RawDoublesPseudoSketch pseudoSketch,
      Supplier<UpdateDoublesSketch> sketchSupplier,
      StateUpdateFunction stateUpdateFunction
  )
  {
    return new MetaDoublesSketch(
        pseudoSketch,
        sketchSupplier,
        null,
        State.RAW_DOUBLES,
        stateUpdateFunction
    );
  }

  public static MetaDoublesSketch createFromUpdateSketch(UpdateDoublesSketch sketch)
  {
    return new MetaDoublesSketch(
        null,
        () -> sketch,
        null,
        State.UPDATE_SKETCH,
        StateUpdateFunction.NO_OP
    );
  }

  public static MetaDoublesSketch createFromSketch(@Nullable DoublesSketch sketch)
  {
    return new MetaDoublesSketch(
        null,
        MetaDoublesSketchUtil.SketchUoeSupplier(),
        sketch,
        State.SKETCH,
        StateUpdateFunction.NO_OP
    );
  }

  public static MetaDoublesSketch fromWireBytes(byte[] bytes)
  {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());

    return wrapMemoryBuffer(byteBuffer, 0, MetaDoublesSketchUtil.UpdateSketchUoeMemoryAccessor());
  }

  public static MetaDoublesSketch wrapMemoryBuffer(
      ByteBuffer originalByteBuffer,
      int position,
      MemoryAccessor<UpdateDoublesSketch> updateSketchAccessor
  )
  {
    BufferEncoding bufferEncoding = BufferEncoding.fromOrdinal(originalByteBuffer.get(position));
    ByteBuffer objectByteBuffer = originalByteBuffer.duplicate().order(ByteOrder.nativeOrder());

    objectByteBuffer.position(position + MEMORY_BUFFER_HEADER_SIZE);

    switch (bufferEncoding) {
      case UPDATE_SKETCH:
        UpdateDoublesSketch updateDoublesSketch = updateSketchAccessor.wrap(
            originalByteBuffer,
            position + MetaDoublesSketch.MEMORY_BUFFER_HEADER_SIZE
        );

        return MetaDoublesSketch.createFromUpdateSketch(updateDoublesSketch);
      case SKETCH:
        DoublesSketch sketch = DoublesSketch.wrap(WritableMemory.writableWrap(objectByteBuffer));

        return MetaDoublesSketch.createFromSketch(sketch);
      case RAW_DOUBLES:
        DirectRawDoublesPseudoSketch pseudoSketch =
            DirectRawDoublesPseudoSketch.wrapDefaultMaxInstance(objectByteBuffer);

        return MetaDoublesSketch.createFromRawDoublesBuffer(
            pseudoSketch,
            () -> updateSketchAccessor.initAndWrap(originalByteBuffer, position + Byte.BYTES),
            new BufferEncodingStateUpdateFunction(objectByteBuffer, position)
        );
      default:
        throw new IAE("%s: unhandled encoding %s", MetaDoublesSketch.class.getName(), bufferEncoding);
    }
  }

  public static void initNewBuffer(ByteBuffer originalByteBuffer, int position)
  {
    ByteBuffer byteBuffer = originalByteBuffer.duplicate().order(originalByteBuffer.order());

    byteBuffer.position(position);
    byteBuffer.put(BufferEncoding.RAW_DOUBLES.toByte());
    DirectRawDoublesPseudoSketch.initEmptyDefaultMaxInstanceBuffer(
        byteBuffer,
        byteBuffer.position()
    );
  }

  public void update(double value)
  {
    switch (state) {
      case RAW_DOUBLES:
        if (getPseudoSketch().hasSpace()) {
          getPseudoSketch().update(value);
        } else {
          changeToUpdateSketch();
          getUpdateSketch().update(value);
        }
        break;
      case UPDATE_SKETCH:
        getUpdateSketch().update(value);
        break;
      case SKETCH:
        throw new UOE("read-only sketch");
      default:
        throw new ISE("unhandled state %s", state);
    }
  }

  public void pushInto(MetaDoublesUnion metaDoublesUnion)
  {
    switch (state) {
      case RAW_DOUBLES:
        getPseudoSketch().pushInto(metaDoublesUnion);
        break;
      case UPDATE_SKETCH:
      case SKETCH:
        metaDoublesUnion.update(getSketch());
        break;
      default:
        throw new ISE("unhandled state  %s", state);
    }

  }

  public boolean isEmpty()
  {
    switch (state) {
      case RAW_DOUBLES:
        return getPseudoSketch().getRawDoublesCount() == 0;
      case UPDATE_SKETCH:
      case SKETCH:
        return getSketch().isEmpty();
      default:
        throw new ISE("unhandled state  %s", state);
    }

  }

  public DoublesSketch asSketch()
  {
    switch (state) {
      case RAW_DOUBLES:
        changeToUpdateSketch();

        return getUpdateSketch();
      case UPDATE_SKETCH:
        return getUpdateSketch();
      case SKETCH:
        return getSketch();
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }

  public void relocate(
      int oldPosition,
      int newPosition,
      ByteBuffer oldBuffer,
      ByteBuffer newBuffer,
      MemoryAccessor<UpdateDoublesSketch> sketchMemoryAccessor
  )
  {
    ByteBuffer destinationBuffer = newBuffer.duplicate().order(newBuffer.order());
    destinationBuffer.position(newPosition + MEMORY_BUFFER_HEADER_SIZE);

    newBuffer.put(newPosition, state.getBufferEncoding().toByte());

    switch (state) {
      case RAW_DOUBLES:
        getPseudoSketch().store(destinationBuffer);
        break;
      case UPDATE_SKETCH:
        sketchMemoryAccessor.relocate(
            oldPosition + MEMORY_BUFFER_HEADER_SIZE,
            newPosition + MEMORY_BUFFER_HEADER_SIZE,
            oldBuffer,
            newBuffer
        );
        break;
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }

  public void copyTo(ByteBuffer byteBuffer, int position)
  {
    switch (state) {
      case RAW_DOUBLES:
        getPseudoSketch().store(byteBuffer);
        break;
      case SKETCH:
        byteBuffer.put(getUpdateSketch().toByteArray(true));
        break;
      case UPDATE_SKETCH:
        byteBuffer.put(getSketch().toByteArray(true));
        break;
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }


  /**
   * this method is used for storing in durable storage (column segments)
   *
   * @param byteBuffer
   */
  public void store(ByteBuffer byteBuffer)
  {
    switch (state) {
      case SKETCH:
        byteBuffer.put(getUpdateSketch().toByteArray(true));
        break;
      case UPDATE_SKETCH:
        byteBuffer.put(getSketch().toByteArray(true));
        break;
      case RAW_DOUBLES:
        getPseudoSketch().store(byteBuffer);
        break;
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }

  @JsonValue
  public byte[] toWireBytes()
  {
    return toWireBytes(true);
  }

  // this method is for over-the-wire format (more compact header)
  public byte[] toWireBytes(boolean compact)
  {
    ByteBuffer byteBuffer;
    switch (state) {
      case RAW_DOUBLES:
        int bytesSize = getPseudoSketch().getSerializedSize();
        byte[] bytes = new byte[bytesSize + MEMORY_BUFFER_HEADER_SIZE];
        byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());

        byteBuffer.put(state.getBufferEncoding().toByte());
        getPseudoSketch().store(byteBuffer);

        return bytes;
      case UPDATE_SKETCH:
        // an UpdateDoublesSketch is converted to a read-only, compact sketch for over-the-wire
      case SKETCH:
        byte[] sketchBytes = getSketch().toByteArray(compact);
        byte[] bytesWithHeader = new byte[sketchBytes.length + MEMORY_BUFFER_HEADER_SIZE];

        byteBuffer = ByteBuffer.wrap(bytesWithHeader);
        byteBuffer.put(State.SKETCH.getBufferEncoding().toByte());
        byteBuffer.put(sketchBytes);

        return sketchBytes;
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private UpdateDoublesSketch getUpdateSketch()
  {
    Preconditions.checkState(state == State.UPDATE_SKETCH, "asking for update sketch when in state %s", state);

    return updateSketchSupplier.get();
  }

  // returns either the UpdateDoublesSketch or DoublesSketch based on state
  private DoublesSketch getSketch()
  {
    Preconditions.checkState(
        state == State.UPDATE_SKETCH || state == State.SKETCH,
        "asking for sketch when in state %s",
        state
    );

    if (state == State.UPDATE_SKETCH) {
      return getUpdateSketch();
    } else {
      return sketch;
    }
  }

  private RawDoublesPseudoSketch getPseudoSketch()
  {
    Preconditions.checkState(state == State.RAW_DOUBLES, "asking for raw doubles when in state %s", state);

    return pseudoSketch;
  }

  @SuppressWarnings("ConstantConditions")
  private void changeToUpdateSketch()
  {
    if (state == State.UPDATE_SKETCH) {
      return;
    }

    Preconditions.checkState(state == State.RAW_DOUBLES, "invalid transition %s to %s", state, State.UPDATE_SKETCH);
    // in the case of off-heap storage, we need to copy to the heap before we use the sketchSupplier to initialize
    // the memory for use by an off-heap UpdateDoublesSketch. When operating with heap object, onHeap() returns
    // the same object
    RawDoublesPseudoSketch heapPseudoSketch = pseudoSketch.onHeap();
    UpdateDoublesSketch updateDoublesSketch = updateSketchSupplier.get();

    heapPseudoSketch.pushInto(updateDoublesSketch);
    pseudoSketch = null;
    state = State.UPDATE_SKETCH;
    stateUpdateFunction.update(State.UPDATE_SKETCH.getBufferEncoding());
  }

  private enum State
  {
    RAW_DOUBLES(BufferEncoding.RAW_DOUBLES),
    UPDATE_SKETCH(BufferEncoding.UPDATE_SKETCH),
    SKETCH(BufferEncoding.SKETCH),
    ;

    private final BufferEncoding encoding;

    State(BufferEncoding encoding)
    {
      this.encoding = encoding;
    }

    private BufferEncoding getBufferEncoding()
    {
      return encoding;
    }
  }
}
