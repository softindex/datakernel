/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.transformers;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.global.common.PubKey;

/**
 * Converts stream of frames to a stream of bytebufs of pure data.
 * Does the checkpoint verification and so on.
 * Also it cuts out the checkpoint paddings out of the resulting stream.
 * <p>
 * It's counterpart is the {@link FrameSigner}.
 */
public final class FrameVerifier extends FramesToByteBufs {
	private final long offset;
	private final long endOffset;

	// region creators
	public FrameVerifier(PubKey pubKey, long offset, long length) {
		super(pubKey);
		this.offset = offset;
		this.endOffset = length == -1 ? Long.MAX_VALUE : offset + length;
	}
	// endregion

	@Override
	protected Stage<Void> receiveByteBuffer(ByteBuf byteBuf) {
		int size = byteBuf.readRemaining();
		if (position <= offset || position - size > endOffset) {
			return Stage.of(null);
		}
		if (position - size < offset) {
			byteBuf.moveReadPosition((int) (offset - position + size));
		}
		if (position > endOffset) {
			byteBuf.moveWritePosition((int) (endOffset - position));
		}
		return output.accept(byteBuf);
	}
}
