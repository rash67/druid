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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * <pre>
 * this object uses a state pattern. It may be initialized in a variety of states, and certain methods may not be
 * available in all states (eg SKETCH does not allow update() as it is read-only).
 *
 * The states:
 *    RAW_DOUBLES: in build aggregators, this is always the intialized state. It collects doubles in a list
 *    (either on-heap or off-heap, depending which is supplied) until a max size is reached. At that point, it
 *    will use a provided supplier to create an UpdateDoublesSketch, push the doubles into it, and then behave
 *    as UpdateDoublesSketch.
 *
 *    UPDATE_SKETCH: an UpdateDoublesSketch has been directly supplied (instance supplier) or else created from a
 *    supplier function. This is writable and values may be added
 *
 *    SKETCH: this is only created directly when a sketch is supplied. This is basically just wrapping the
 *    DoublesSketch in order to pass around a consistent object, this MetaDoublesSketch
 *
 * This object also knows how to efficiency push into a {@link MetaDoublesUnion}. Depending on the state, it
 * will call update(double) or update(DoubleSketch). See {@link MetaDoublesUnion} as it also uses a
 * similar, but simpler state pattern.
 *
 * </pre>
 */
public class MetaDoublesUnion
{
  public static int MEMORY_BUFFER_HEADER_SIZE = Byte.BYTES;

  @Nullable
  private RawDoublesPseudoSketch pseudoSketch;
  private final Supplier<DoublesUnion> unionSupplier;
  private final StateUpdateFunction stateUpdateFunction;

  private State state;

  private MetaDoublesUnion(
      @Nullable
      RawDoublesPseudoSketch pseudoSketch,
      Supplier<DoublesUnion> unionSupplier,
      State initialState,
      StateUpdateFunction stateUpdateFunction
  )
  {
    this.pseudoSketch = pseudoSketch;
    this.unionSupplier = Suppliers.memoize(unionSupplier);
    state = initialState;
    this.stateUpdateFunction = stateUpdateFunction;
  }

  public static MetaDoublesUnion createFromRawDoubles(
      RawDoublesPseudoSketch pseudoSketch,
      DoublesUnion union
  )
  {
    return new MetaDoublesUnion(
        pseudoSketch,
        () -> union,
        State.RAW_DOUBLES,
        StateUpdateFunction.NO_OP
    );
  }

  public static MetaDoublesUnion createFromUnion(DoublesUnion union)
  {
    return new MetaDoublesUnion(
        null,
        () -> union,
        State.UNION,
        StateUpdateFunction.NO_OP
    );
  }

  public static void initNewBuffer(ByteBuffer originalByteBuffer, int position)
  {
    ByteBuffer byteBuffer = originalByteBuffer.duplicate().order(originalByteBuffer.order());

    byteBuffer.position(position);
    byteBuffer.put(State.RAW_DOUBLES.getBufferEncoding().toByte());

    DirectRawDoublesPseudoSketch.initEmptyDefaultMaxInstanceBuffer(
        byteBuffer,
        byteBuffer.position()
    );
  }

  public static MetaDoublesUnion wrapMemoryBuffer(
      ByteBuffer byteBuffer,
      int position,
      MemoryAccessor<DoublesUnion> doublesUnionAccessor
  )
  {
    BufferEncoding bufferEncoding = BufferEncoding.fromOrdinal(byteBuffer.get(position));

    switch (bufferEncoding) {
      case RAW_DOUBLES:
        DirectRawDoublesPseudoSketch pseudoSketch =
            DirectRawDoublesPseudoSketch.wrapDefaultMaxInstance(byteBuffer);

        return MetaDoublesUnion.createFromRawDoublesBuffer(
            pseudoSketch,
            () -> doublesUnionAccessor.initAndWrap(byteBuffer, position + Byte.BYTES),
            new BufferEncodingStateUpdateFunction(byteBuffer, position)
        );

      case UNION:
        return MetaDoublesUnion.createFromUnion(doublesUnionAccessor.wrap(byteBuffer, position + Byte.BYTES));

      default:
        throw new IAE("unhandled encoding %s", bufferEncoding);
    }
  }

  private static MetaDoublesUnion createFromRawDoublesBuffer(
      DirectRawDoublesPseudoSketch pseudoSketch,
      Supplier<DoublesUnion> unionSupplier,
      StateUpdateFunction stateUpdateFunction
  )
  {
    return new MetaDoublesUnion(
        pseudoSketch,
        unionSupplier,
        State.RAW_DOUBLES,
        stateUpdateFunction
    );
  }

  public void update(RawDoublesPseudoSketch pseudoSketch)
  {
    switch (state) {
      case RAW_DOUBLES:
        if (getPseudoSketch().hasSpaceFor(pseudoSketch.getRawDoublesCount())) {
          pseudoSketch.pushInto(getPseudoSketch());
        } else {
          changeToUnion();
          pseudoSketch.pushInto(getUnion());
        }
        break;
      case UNION:
        pseudoSketch.pushInto(getUnion());
        break;
      default:
        throw new ISE("unhandled state %s", state);
    }
  }

  public void update(DoublesSketch sketch)
  {
    switch (state) {
      case RAW_DOUBLES:
        changeToUnion();
      case UNION:
        getUnion().update(sketch);
        break;
      default:
        throw new ISE("unhandled state %s", state);
    }
  }

  public void update(double value)
  {
    switch (state) {
      case RAW_DOUBLES:
        if (getPseudoSketch().hasSpace()) {
          getPseudoSketch().update(value);
        } else {
          changeToUnion();
          getUnion().update(value);
        }
        break;
      case UNION:
        getUnion().update(value);
        break;
      default:
        throw new ISE("unhandled state %s", state);
    }
  }

  public MetaDoublesSketch toMetaDoublesSketch()
  {
    switch (state) {
      case RAW_DOUBLES:
        changeToUnion();
      case UNION:
        return MetaDoublesSketch.createFromSketch(getUnion().getResult());
      default:
        throw new ISE("unhandled state  %s", state);
    }
  }

  public void relocate(
      int oldPosition,
      int newPosition,
      ByteBuffer oldBuffer,
      ByteBuffer newBuffer,
      MemoryAccessor<DoublesUnion> unionMemoryAccessor
  )
  {
    ByteBuffer destinationBuffer = newBuffer.duplicate().order(ByteOrder.nativeOrder());
    destinationBuffer.position(newPosition + MEMORY_BUFFER_HEADER_SIZE);

    newBuffer.put(newPosition, state.getBufferEncoding().toByte());

    switch (state) {
      case RAW_DOUBLES:
        getPseudoSketch().store(destinationBuffer);
        break;
      case UNION:
        unionMemoryAccessor.relocate(
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


  private RawDoublesPseudoSketch getPseudoSketch()
  {
    Preconditions.checkState(state == State.RAW_DOUBLES, "asking for pseudoSketch when in state %s", state);

    return pseudoSketch;
  }

  private DoublesUnion getUnion()
  {
    Preconditions.checkState(state == State.UNION, "asking for union when in state %s", state);

    return unionSupplier.get();
  }

  @SuppressWarnings("ConstantConditions")
  private void changeToUnion()
  {
    if (state == State.UNION) {
      return;
    }
    RawDoublesPseudoSketch heapPseudoSketch = pseudoSketch.onHeap();

    heapPseudoSketch.pushInto(unionSupplier.get());
    pseudoSketch = null;
    state = State.UNION;
    stateUpdateFunction.update(state.getBufferEncoding());
  }

  private enum State
  {
    RAW_DOUBLES(BufferEncoding.RAW_DOUBLES, MetaDoublesSketchHeader.Encoding.RAW_DOUBLES),
    UNION(BufferEncoding.UNION, MetaDoublesSketchHeader.Encoding.SKETCH),
    ;

    private final BufferEncoding bufferEncoding;
    private final MetaDoublesSketchHeader.Encoding headerEncoding;

    State(BufferEncoding bufferEncoding, MetaDoublesSketchHeader.Encoding headerEncoding)
    {
      this.bufferEncoding = bufferEncoding;
      this.headerEncoding = headerEncoding;
    }

    private MetaDoublesSketchHeader.Encoding getHeaderEncoding()
    {
      return headerEncoding;
    }

    public BufferEncoding getBufferEncoding()
    {
      return bufferEncoding;
    }
  }
}
