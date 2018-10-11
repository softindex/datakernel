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

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.*;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;

/**
 * Decodes a stream of byte bufs back into a stream of frames.
 * <p>
 * It's counterpart is the {@link FrameEncoder}.
 */
public final class FrameDecoder extends AbstractAsyncProcess implements WithSerialToSerial<FrameDecoder, ByteBuf, DataFrame> {
	protected SerialSupplier<ByteBuf> input;
	protected SerialConsumer<DataFrame> output;

	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			if (this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	public SerialOutput<DataFrame> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null) startProcess();
		};
	}

	@Override
	protected void doProcess() {
		ByteBufsSupplier.of(input)
				.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes().andThen(this::parseDataFrame))
				.streamTo(output)
				.whenResult($ -> completeProcess());
	}

	@Override
	protected final void doCloseWithError(Throwable e) {
		input.close(e);
		output.close(e);
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
