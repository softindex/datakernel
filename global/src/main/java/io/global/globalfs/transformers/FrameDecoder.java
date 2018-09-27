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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.processor.AbstractSerialTransformer;
import io.global.common.SignedData;
import io.global.globalfs.api.DataFrame;
import io.global.globalfs.api.GlobalFsCheckpoint;

import java.io.IOException;

/**
 * Decodes a stream of byte bufs back into a stream of frames.
 * <p>
 * It's counterpart is the {@link FrameEncoder}.
 */
public final class FrameDecoder extends AbstractSerialTransformer<FrameDecoder, ByteBuf, DataFrame> {

	@Nullable
	private static DataFrame parseDataFrame(ByteBufQueue bbq) throws ParseException {
		if (!bbq.hasRemainingBytes(6)) { // TODO anton: do something with that 6
			return null;
		}
		byte type = bbq.peekByte();
		assert type == 0 || type == 1;
		int size = bbq.peekVarInt(1);
		if (!bbq.hasRemainingBytes(size)) {
			return null;
		}
		ByteBuf data = bbq.takeExactSize(size);
		if (type == 0) {
			return DataFrame.of(data);
		}
		try {
			return DataFrame.of(SignedData.ofBytes(data.asArray(), GlobalFsCheckpoint::ofBytes));
		} catch (IOException e) {
			throw new ParseException(e);
		}
	}

	@Override
	protected void doProcess() {
		ByteBufsSupplier.of(input).parseStream(FrameDecoder::parseDataFrame).streamTo(output);
	}
}
