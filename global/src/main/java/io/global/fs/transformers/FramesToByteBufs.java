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
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsCheckpoint.CheckpointVerificationResult;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;

abstract class FramesToByteBufs extends AbstractAsyncProcess
		implements WithSerialToSerial<FramesToByteBufs, DataFrame, ByteBuf> {
	private final PubKey pubKey;
	private final String filename;

	protected SerialSupplier<DataFrame> input;
	protected SerialConsumer<ByteBuf> output;
	protected long position = 0;

	private boolean first = true;
	private SHA256Digest digest;

	FramesToByteBufs(PubKey pubKey, String filename) {
		this.pubKey = pubKey;
		this.filename = filename;
	}

	@Override
	public SerialInput<DataFrame> getInput() {
		return input -> {
			this.input = sanitize(input);
			if (this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	public SerialOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null) startProcess();
		};
	}

	@Override
	protected void doProcess() {
		input.get()
				.thenCompose(frame ->
						frame != null ?
								handleFrame(frame)
										.whenResult($ -> doProcess()) :
								output.accept(null)
										.whenResult($ -> completeProcess()))
				.whenException(this::close);
	}

	protected Promise<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
		return Promise.of(null);
	}

	protected Promise<Void> receiveByteBuffer(ByteBuf byteBuf) {
		return output.accept(byteBuf);
	}

	private Promise<Void> handleFrame(DataFrame frame) {
		if (first) {
			first = false;
			if (!frame.isCheckpoint()) {
				return Promise.ofException(new IOException("First dataframe is not a checkpoint!"));
			}
			GlobalFsCheckpoint data = frame.getCheckpoint().getData();
			position = data.getPosition();
			digest = new SHA256Digest(data.getDigest());
		}
		if (frame.isCheckpoint()) {
			SignedData<GlobalFsCheckpoint> checkpoint = frame.getCheckpoint();
			CheckpointVerificationResult result = GlobalFsCheckpoint.verify(checkpoint, pubKey, position, digest, filename);
			if (result != CheckpointVerificationResult.SUCCESS) {
				return Promise.ofException(new StacklessException(FramesToByteBufs.class, "Checkpoint verification failed: " + result.message));
			}
			// return output.post(ByteBuf.wrapForReading(new byte[]{124}));
			return receiveCheckpoint(checkpoint);
		}
		ByteBuf buf = frame.getBuf();
		int size = buf.readRemaining();
		position += size;
		digest.update(buf.array(), buf.readPosition(), size);
		return receiveByteBuffer(buf);
	}

	@Override
	protected final void doCloseWithError(Throwable e) {
		if (input != null) {
			input.close(e);
		}
		if (output != null) {
			output.close(e);
		}
	}
}
