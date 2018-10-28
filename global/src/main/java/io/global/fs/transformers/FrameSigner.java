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
import io.global.common.PrivKey;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.LocalPath;
import org.spongycastle.crypto.digests.SHA256Digest;

/**
 * Converts a stream of data into a stream of frames.
 * Makes, places and signs the checkpoints using the given strategies and keys.
 * <p>
 * It's counterpart is the {@link FrameVerifier}.
 */
public final class FrameSigner extends ByteBufsToFrames {
	private final byte[] localPathHash;
	private final CheckpointPosStrategy checkpointPosStrategy;
	private final PrivKey privateKey;
	private final SHA256Digest digest;

	private boolean lastPostedCheckpoint = false;

	// region creators
	public FrameSigner(LocalPath localPath, long offset, CheckpointPosStrategy checkpointPosStrategy, PrivKey privateKey, SHA256Digest digest) {
		super(offset);
		this.localPathHash = localPath.hash();
		this.checkpointPosStrategy = checkpointPosStrategy;
		this.privateKey = privateKey;
		this.digest = digest;
	}
	// endregion

	@Override
	protected Promise<Void> postByteBuf(ByteBuf buf) {
		int size = buf.readRemaining();
		digest.update(buf.array(), buf.readPosition(), size);
		lastPostedCheckpoint = false;
		return super.postByteBuf(buf);
	}

	@Override
	protected Promise<Void> postNextCheckpoint() {
		nextCheckpoint = checkpointPosStrategy.nextPosition(nextCheckpoint);
		GlobalFsCheckpoint checkpoint = GlobalFsCheckpoint.of(position, new SHA256Digest(digest), localPathHash);
		lastPostedCheckpoint = true;
		return output.accept(DataFrame.of(SignedData.sign(checkpoint, privateKey)));
	}

	@Override
	protected void iteration() {
		input.get()
				.thenCompose(buf ->
						buf != null ?
								handleBuffer(buf)
										.whenResult($ -> iteration()) :
								(lastPostedCheckpoint ?
										output.accept(null) :
										postNextCheckpoint()
												.thenCompose($ -> output.accept(null)))
										.whenResult($ -> completeProcess()))
				.whenException(this::close);
	}
}
