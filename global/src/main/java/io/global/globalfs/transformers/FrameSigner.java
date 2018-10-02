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
import io.global.common.PrivKey;
import io.global.common.SignedData;
import io.global.globalfs.api.CheckpointPositionStrategy;
import io.global.globalfs.api.DataFrame;
import io.global.globalfs.api.GlobalFsCheckpoint;
import org.spongycastle.crypto.digests.SHA256Digest;

/**
 * Converts a stream of data into a stream of frames.
 * Makes, places and signs the checkpoints using the given strategies and keys.
 * <p>
 * It's counterpart is the {@link FrameVerifier}.
 */
public final class FrameSigner extends ByteBufsToFrames {
	private final SHA256Digest digest = new SHA256Digest();
	private final CheckpointPositionStrategy checkpointPositionStrategy;
	private final PrivKey privateKey;

	private boolean lastPostedCheckpoint = false;

	// region creators
	public FrameSigner(long offset, CheckpointPositionStrategy checkpointPositionStrategy, PrivKey privateKey) {
		super(offset);
		this.checkpointPositionStrategy = checkpointPositionStrategy;
		this.privateKey = privateKey;
	}
	// endregion

	@Override
	protected Stage<Void> postByteBuf(ByteBuf buf) {
		digest.update(buf.array(), buf.readPosition(), buf.readRemaining());
		lastPostedCheckpoint = false;
		return super.postByteBuf(buf);
	}

	@Override
	protected Stage<Void> postNextCheckpoint() {
		nextCheckpoint = checkpointPositionStrategy.nextPosition(nextCheckpoint);
		GlobalFsCheckpoint checkpoint = GlobalFsCheckpoint.of(position, new SHA256Digest(digest));
		lastPostedCheckpoint = true;
		return output.accept(DataFrame.of(SignedData.sign(checkpoint, privateKey)));
	}

	@Override
	protected void iteration() {
		input.get()
				.thenCompose(buf ->
						buf != null ?
								handleBuffer(buf).thenRun(this::iteration) :
								(lastPostedCheckpoint ?
										output.accept(null) :
										postNextCheckpoint()
												.thenCompose($ -> output.accept(null)))
										.thenRun(this::completeProcess))
				.whenException(this::closeWithError);
	}
}
