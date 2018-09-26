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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.serial.processor.SimpleSerialTransformer;
import io.global.globalfs.api.DataFrame;

/**
 * Encodes stream of frames into a stream of bytebufs to transmit or store those frames.
 * <p>
 * It's counterpart is the {@link FrameDecoder}.
 */
public final class FrameEncoder extends SimpleSerialTransformer<FrameEncoder, DataFrame, ByteBuf> {

	private static final byte[] DATA_HEADER = new byte[]{0};
	private static final byte[] CHECKPOINT_HEADER = new byte[]{1};

	@Override
	protected Stage<Void> handle(DataFrame frame) {
		ByteBuf data = frame.isBuf() ? frame.getBuf() : ByteBuf.wrapForReading(frame.getCheckpoint().getData().toBytes());
		ByteBuf sizeBuf = ByteBufPool.allocate(5);
		sizeBuf.writeVarInt(data.readRemaining());
		return Stage.complete()
				.thenCompose($ -> output.accept(ByteBuf.wrapForReading(frame.isBuf() ? DATA_HEADER : CHECKPOINT_HEADER)))
				.thenCompose($ -> output.accept(sizeBuf)) // anyway this will be repacked by socket components,
				.thenCompose($ -> output.accept(data));   // or stored in OS buffer when writing to disk
	}
}
