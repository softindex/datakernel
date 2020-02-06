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

package io.datakernel.http.stream;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.csp.*;
import io.datakernel.csp.dsl.WithChannelTransformer;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.common.Preconditions.checkState;

/**
 * This is a binary channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * converting some raw data into its <a href="https://tools.ietf.org/html/rfc2616#section-3.6.1">chunked transfer encoding<a> form.
 */
public final class BufsConsumerChunkedEncoder extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerChunkedEncoder, ByteBuf, ByteBuf> {
	private final ByteBuf LAST_CHUNK = ByteBuf.wrapForReading(new byte[]{48, 13, 10, 13, 10});

	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedEncoder() {
	}

	public static BufsConsumerChunkedEncoder create() {
		return new BufsConsumerChunkedEncoder();
	}

	@SuppressWarnings("ConstantConditions") //check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						if (buf.canRead()) {
							output.accept(encodeBuf(buf))
									.whenResult(this::doProcess);
						} else {
							buf.recycle();
							doProcess();
						}
					} else {
						output.accept(LAST_CHUNK, null)
								.whenResult(() -> completeProcess());
					}
				});
	}

	private static ByteBuf encodeBuf(ByteBuf buf) {
		int bufSize = buf.readRemaining();
		char[] hexRepr = Integer.toHexString(bufSize).toCharArray();
		int hexLen = hexRepr.length;
		ByteBuf chunkBuf = ByteBufPool.allocate(hexLen + 2 + bufSize + 2);
		byte[] chunkArray = chunkBuf.array();
		for (int i = 0; i < hexLen; i++) {
			chunkArray[i] = (byte) hexRepr[i];
		}
		chunkArray[hexLen] = CR;
		chunkArray[hexLen + 1] = LF;
		chunkBuf.tail(hexLen + 2);
		chunkBuf.put(buf);
		buf.recycle();
		chunkBuf.writeByte(CR);
		chunkBuf.writeByte(LF);
		return chunkBuf;
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		output.close(e);
	}

}
