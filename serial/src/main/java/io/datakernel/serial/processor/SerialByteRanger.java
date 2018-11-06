/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.serial.processor;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;

/** Drops exactly N bytes from a serial strem of byte buffers and limits that stream to exactly M bytes in length */
public final class SerialByteRanger extends SerialTransformer<SerialByteRanger, ByteBuf, ByteBuf> {
	private final long offset;
	private final long endOffset;

	private long position = 0;

	private SerialByteRanger(long offset, long length) {
		this.offset = offset;
		this.endOffset = length;
	}

	public static SerialBidiFunction<ByteBuf, ByteBuf> range(long offset, long length) {
		if (offset == 0 && length == Long.MAX_VALUE) {
			return SerialBidiFunction.identity();
		}
		return new SerialByteRanger(offset, length);
	}

	public static SerialBidiFunction<ByteBuf, ByteBuf> drop(long toDrop) {
		return range(toDrop, Long.MAX_VALUE);
	}

	public static SerialBidiFunction<ByteBuf, ByteBuf> limit(long limit) {
		return range(0, limit);
	}

	@Override
	protected Promise<Void> onItem(ByteBuf item) {
		int size = item.readRemaining();
		long oldPos = position;
		position += size;
		if (oldPos > endOffset || position <= offset) {
			item.recycle();
			return Promise.complete();
		}
		if (oldPos < offset) {
			item.moveReadPosition((int) (offset - oldPos));
		}
		if (position > endOffset) {
			item.moveWritePosition((int) (endOffset - position));
		}
		return send(item);
	}
}
