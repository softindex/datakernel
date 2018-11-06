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
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.processor.SerialTransformer;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;

/**
 * Decodes a stream of byte bufs back into a stream of frames.
 * <p>
 * It's counterpart is the {@link FrameEncoder}.
 */
public final class FrameDecoder extends SerialTransformer<FrameDecoder, ByteBuf, DataFrame> {

	@Override
	protected Promise<Void> onItem(ByteBuf item) {
		return Promise.complete();
	}

	@Override
	protected void doProcess() {
		ByteBufsSupplier.of(input)
				.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes().andThen(this::parseDataFrame))
				.streamTo(output)
				.whenResult($ -> completeProcess())
				.whenException(this::close);
	}

	private DataFrame parseDataFrame(ByteBuf buf) throws ParseException {
		byte type = buf.readByte();
		assert type == 0 || type == 1;
		if (type == 0) {
			return DataFrame.of(buf);
		}
		return DataFrame.of(SignedData.ofBytes(buf.asArray(), GlobalFsCheckpoint::ofBytes));
	}
}
