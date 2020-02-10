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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.promise.Promise;
import io.global.common.Hash;
import io.global.common.PrivKey;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.digests.SHA256Digest;

import static io.global.fs.util.BinaryDataFormats.REGISTRY;

/**
 * Converts a stream of data into a stream of frames.
 * Makes, places and signs the checkpoints using the given strategies and keys.
 * <p>
 * It's counterpart is the {@link FrameVerifier}.
 */
public final class FrameSigner extends ByteBufsToFrames {
	private static final StructuredCodec<GlobalFsCheckpoint> CHECKPOINT_CODEC = REGISTRY.get(GlobalFsCheckpoint.class);

	private final String filename;
	private final CheckpointPosStrategy checkpointPosStrategy;
	private final PrivKey privateKey;
	private final long revision;
	private final SHA256Digest digest;

	@Nullable
	private final Hash simKeyHash;

	private boolean lastPostedCheckpoint = false;

	private FrameSigner(PrivKey privateKey, CheckpointPosStrategy checkpointPosStrategy,
						String filename, long offset, long revision, SHA256Digest digest, @Nullable Hash simKeyHash) {
		super(offset);
		this.filename = filename;
		this.checkpointPosStrategy = checkpointPosStrategy;
		this.privateKey = privateKey;
		this.revision = revision;
		this.digest = digest;
		this.simKeyHash = simKeyHash;
	}

	public static FrameSigner create(PrivKey privateKey, CheckpointPosStrategy checkpointPosStrategy,
									 String filename, long offset, long revision, @Nullable SHA256Digest digest, @Nullable Hash simKeyHash) {
		return new FrameSigner(privateKey, checkpointPosStrategy, filename, offset, revision, digest != null ? digest : new SHA256Digest(), simKeyHash);
	}

	@Override
	protected Promise<Void> postByteBuf(ByteBuf buf) {
		int size = buf.readRemaining();
		position += size;
		digest.update(buf.array(), buf.head(), size);
		lastPostedCheckpoint = false;
		return send(DataFrame.of(buf));
	}

	@Override
	protected Promise<Void> postCheckpoint() {
		nextCheckpoint = checkpointPosStrategy.nextPosition(nextCheckpoint);
		GlobalFsCheckpoint checkpoint = GlobalFsCheckpoint.of(filename, position, revision, new SHA256Digest(digest), simKeyHash);
		lastPostedCheckpoint = true;
		return send(DataFrame.of(SignedData.sign(CHECKPOINT_CODEC, checkpoint, privateKey)));
	}

	@Override
	protected Promise<Void> onProcessFinish() {
		return lastPostedCheckpoint ? sendEndOfStream() : postCheckpoint().then($ -> sendEndOfStream());
	}
}
