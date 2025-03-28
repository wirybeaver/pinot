/**
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
package org.apache.pinot.segment.local.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public final class FixedBitIntReaderWriter implements Closeable {
  private final PinotDataBitSet _dataBitSet;
  private final int _numBitsPerValue;

  public FixedBitIntReaderWriter(PinotDataBuffer dataBuffer, int numValues, int numBitsPerValue) {
    long actualBufferSize = dataBuffer.size();
    long expectedBufferSize = ((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    Preconditions.checkState(actualBufferSize == expectedBufferSize, "Buffer size mismatch: actual: %s, expected: %s",
        actualBufferSize, expectedBufferSize);
    Preconditions.checkState(actualBufferSize <= Integer.MAX_VALUE, "Buffer size too large: %s", actualBufferSize);
    _dataBitSet = new PinotDataBitSet(dataBuffer);
    _numBitsPerValue = numBitsPerValue;
  }

  public int readInt(int index) {
    return _dataBitSet.readInt(index, _numBitsPerValue);
  }

  public void readInt(int startIndex, int length, int[] buffer) {
    _dataBitSet.readInt(startIndex, _numBitsPerValue, length, buffer);
  }

  public void writeInt(int index, int value) {
    _dataBitSet.writeInt(index, _numBitsPerValue, value);
  }

  public void writeInt(int startIndex, int length, int[] values) {
    _dataBitSet.writeInt(startIndex, _numBitsPerValue, length, values);
  }

  public int getStartByteOffset(int index) {
    return (int) (((long) index * _numBitsPerValue) / Byte.SIZE);
  }

  public int getEndByteOffset(int index) {
    return (int) (((long) (index + 1) * _numBitsPerValue - 1) / Byte.SIZE) + 1;
  }

  @Override
  public void close() {
    _dataBitSet.close();
  }
}
