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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.process.AbstractChannelTransformer;
import io.datakernel.util.TypeT;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.global.ot.util.BinaryDataFormats2.REGISTRY;

/**
 * Encodes stream of frames into a stream of bytebufs to transmit or store those frames.
 * <p>
 * It's counterpart is the {@link FrameDecoder}.
 */
public final class FrameEncoder extends AbstractChannelTransformer<FrameEncoder, DataFrame, ByteBuf> {
	private static final StructuredCodec<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsCheckpoint>>() {});

	@Override
	protected Promise<Void> onItem(DataFrame item) {
		ByteBuf data = item.isBuf() ? item.getBuf() : encode(SIGNED_CHECKPOINT_CODEC, item.getCheckpoint());
		ByteBuf header = ByteBufPool.allocate(5 + 1);
		header.writeVarInt(data.readRemaining() + 1); // + 1 is for that tag byte below
		header.writeByte((byte) (item.isBuf() ? 0 : 1));
		return send(header).thenCompose($ -> send(data));
	}
}
