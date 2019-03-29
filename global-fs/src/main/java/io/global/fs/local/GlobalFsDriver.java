/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.fs.local;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.exception.StacklessException;
import io.datakernel.util.ref.LongRef;
import io.global.common.*;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.List;
import java.util.Objects;

import static io.datakernel.remotefs.FsClient.*;
import static io.datakernel.util.LogUtils.Level.INFO;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class GlobalFsDriver {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsDriver.class);

	public static final StacklessException FILE_APPEND_WITH_OTHER_KEY = new StacklessException(GlobalFsDriver.class, "Trying to upload to the file with other symmetric key");

	private static final StructuredCodec<GlobalFsCheckpoint> CHECKPOINT_CODEC = REGISTRY.get(GlobalFsCheckpoint.class);
	protected final GlobalFsNode node;

	private final CheckpointPosStrategy checkpointPosStrategy;

	private GlobalFsDriver(GlobalFsNode node, CheckpointPosStrategy checkpointPosStrategy) {
		this.node = node;
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	public static GlobalFsDriver create(GlobalFsNode node, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, checkpointPosStrategy);
	}

	public GlobalFsAdapter adapt(PubKey pubKey) {
		return new GlobalFsAdapter(this, pubKey, null);
	}

	public GlobalFsAdapter adapt(PrivKey privKey) {
		return new GlobalFsAdapter(this, privKey.computePubKey(), privKey);
	}

	public GlobalFsAdapter adapt(KeyPair keys) {
		return new GlobalFsAdapter(this, keys.getPubKey(), keys.getPrivKey());
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(
			KeyPair keys, String filename, long offset, long revision, long skip,
			@Nullable SimKey key, @Nullable SHA256Digest startingDigest) {
		LongRef size = new LongRef(offset + skip);
		return node.upload(keys.getPubKey(), filename, offset + skip, revision)
				.map(consumer -> {
					Hash simKeyHash = key != null ? Hash.sha1(key.getBytes()) : null;
					return consumer
							.transformWith(FrameSigner.create(keys.getPrivKey(), checkpointPosStrategy, filename, offset + skip, revision, startingDigest, simKeyHash))
							.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset + skip))
							.peek(buf -> size.inc(buf.readRemaining()))
							.transformWith(ChannelByteRanger.drop(skip));
				});
	}

	public Promise<ChannelConsumer<ByteBuf>> upload(KeyPair keys, String filename, long offset, long revision, @Nullable SimKey key) {
		if (offset < 0) {
			return Promise.ofException(BAD_RANGE);
		}
		return node.getMetadata(keys.getPubKey(), filename)
				.then(signedCheckpoint -> {
					GlobalFsCheckpoint checkpoint;
					long oldRev;
					if (signedCheckpoint == null || (oldRev = (checkpoint = signedCheckpoint.getValue()).getRevision()) < revision) {
						if (offset != 0) {
							return Promise.ofException(OFFSET_TOO_BIG);
						}
						return doUpload(keys, filename, 0, revision, 0, key, null);
					}
					long size = checkpoint.getPosition();
					if (offset > size) {
						return Promise.ofException(OFFSET_TOO_BIG);
					}
					if (revision < oldRev) {
						return Promise.of(ChannelConsumers.recycling());
					}
					long skip = size - offset;
					if (!Objects.equals(checkpoint.getSimKeyHash(), key != null ? Hash.sha1(key.getBytes()) : null)) {
						return Promise.ofException(FILE_APPEND_WITH_OTHER_KEY);
					}
					return doUpload(keys, filename, offset, revision, skip, key, checkpoint.isTombstone() ? null : checkpoint.getDigest());
				})
				.whenComplete(toLogger(logger, INFO, INFO, "upload", filename, offset, revision, key, this));
	}

	public Promise<ChannelSupplier<ByteBuf>> download(PubKey space, String filename, long offset, long limit) {
		return node.getMetadata(space, filename)
				.then(signedCheckpoint -> {
					if (signedCheckpoint == null) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					GlobalFsCheckpoint metadata = signedCheckpoint.getValue();
					if (metadata.isTombstone()) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					return node.download(space, filename, offset, limit)
							.map(supplier -> supplier.transformWith(FrameVerifier.create(space, filename, offset, limit)));
				})
				.whenComplete(toLogger(logger, INFO, INFO, "download", filename, offset, limit, this));
	}

	public Promise<List<GlobalFsCheckpoint>> listEntities(PubKey space, String glob) {
		return node.listEntities(space, glob)
				.map(list -> list.stream().map(SignedData::getValue).collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "listEntities", glob, this));
	}

	public Promise<List<GlobalFsCheckpoint>> list(PubKey space, String glob) {
		return node.list(space, glob)
				.map(list -> list.stream().map(SignedData::getValue).collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this));
	}

	public Promise<@Nullable GlobalFsCheckpoint> getMetadata(PubKey space, String filename) {
		return node.getMetadata(space, filename)
				.then(signedCheckpoint -> Promise.of(signedCheckpoint != null ? signedCheckpoint.getValue() : null))
				.whenComplete(toLogger(logger, TRACE, "getMetadata", filename, this));
	}

	public Promise<Void> delete(KeyPair keys, String filename, long revision) {
		return node.delete(keys.getPubKey(), SignedData.sign(CHECKPOINT_CODEC, GlobalFsCheckpoint.createTombstone(filename, revision), keys.getPrivKey()))
				.whenComplete(toLogger(logger, TRACE, "delete", filename, revision, this));
	}
}
