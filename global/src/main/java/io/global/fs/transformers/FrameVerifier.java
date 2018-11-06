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

package io.global.fs.transformers;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.global.common.PubKey;
import io.global.fs.api.CheckpointStorage;

/**
 * Something like a splitter, which outputs the bytebuf data, but
 * also stores the checkpoints in given {@link CheckpointStorage}.
 * <p>
 * It's counterpart is the {@link FramesFromStorage}.
 */
public final class FrameVerifier extends FramesToByteBufs {
	private final long offset;
	private final long endOffset;

	private FrameVerifier(PubKey pubKey, String filename, long offset, long endOffset) {
		super(pubKey, filename);
		this.offset = offset;
		this.endOffset = endOffset;
	}

	public static FrameVerifier create(PubKey pubKey, String filename, long offset, long limit) {
		return new FrameVerifier(pubKey, filename, offset, limit == -1 ? Long.MAX_VALUE : offset + limit);
	}

	@SuppressWarnings("Duplicates") // stolen from SerialByteRanger
	@Override
	protected Promise<Void> receiveByteBuf(ByteBuf byteBuf) {
		long oldPos = position - byteBuf.readRemaining();
		if (oldPos > endOffset || position <= offset) {
			byteBuf.recycle();
			return Promise.complete();
		}
		if (oldPos < offset) {
			byteBuf.moveReadPosition((int) (offset - oldPos));
		}
		if (position > endOffset) {
			byteBuf.moveWritePosition((int) (endOffset - position));
		}
		return send(byteBuf);
	}
}
