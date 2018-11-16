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
import io.datakernel.csp.process.AbstractChannelTransformer;
import io.datakernel.exception.StacklessException;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsCheckpoint.CheckpointVerificationResult;
import org.spongycastle.crypto.digests.SHA256Digest;

/**
 * Converts stream of frames to a stream of bytebufs of pure data.
 * Does the checkpoint verification.
 * <p>
 * It's counterpart is the {@link FrameSigner}.
 */
abstract class FramesToByteBufs extends AbstractChannelTransformer<FramesToByteBufs, DataFrame, ByteBuf> {
	private final PubKey pubKey;
	private final String filename;

	protected long position = 0;

	private boolean first = true;
	private SHA256Digest digest = null;

	FramesToByteBufs(PubKey pubKey, String filename) {
		this.pubKey = pubKey;
		this.filename = filename;
	}

	protected Promise<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
		return Promise.complete();
	}

	protected Promise<Void> receiveByteBuf(ByteBuf byteBuf) {
		return send(byteBuf);
	}

	@Override
	protected Promise<Void> onItem(DataFrame frame) {
		if (first) {
			first = false;
			if (!frame.isCheckpoint()) {
				return Promise.ofException(new StacklessException(getClass(), "First dataframe is not a checkpoint!"));
			}
			GlobalFsCheckpoint data = frame.getCheckpoint().getValue();
			position = data.getPosition();
			digest = new SHA256Digest(data.getDigest());
		}
		if (frame.isBuf()) {
			ByteBuf buf = frame.getBuf();
			int size = buf.readRemaining();
			position += size;
			digest.update(buf.array(), buf.readPosition(), size);
			return receiveByteBuf(buf);
		}
		SignedData<GlobalFsCheckpoint> checkpoint = frame.getCheckpoint();
		CheckpointVerificationResult result = GlobalFsCheckpoint.verify(checkpoint, pubKey, filename, position, digest);
		if (result != CheckpointVerificationResult.SUCCESS) {
			return Promise.ofException(new StacklessException(getClass(), "Checkpoint verification failed: " + result.message));
		}
		// return output.post(ByteBuf.wrapForReading(new byte[]{124}));
		return receiveCheckpoint(checkpoint);
	}
}
