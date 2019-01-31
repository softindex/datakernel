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
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.exception.ConstantException;
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

import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.FileUtils.isWildcard;
import static io.datakernel.util.LogUtils.Level.INFO;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static io.global.fs.api.GlobalFsNode.FILE_ALREADY_EXISTS;
import static io.global.fs.api.GlobalFsNode.UPLOADING_TO_TOMBSTONE;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class GlobalFsDriver {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsDriver.class);

	public static final ConstantException UPLOAD_OFFSET_EXCEEDS_FILE_SIZE = new ConstantException(GlobalFsDriver.class, "Trying to upload at offset greater than known file size");
	public static final ConstantException FILE_APPEND_WITH_OTHER_KEY = new ConstantException(GlobalFsDriver.class, "Trying to upload to the file with other symmetric key");

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

	private Promise<ChannelConsumer<ByteBuf>> doUpload(KeyPair keys, String filename, long offset, @Nullable SimKey key, long skip, SHA256Digest startingDigest) {
		long[] size = {offset + skip};
		return node.upload(keys.getPubKey(), filename, offset + skip)
				.thenApply(consumer -> {
					Hash simKeyHash = key != null ? Hash.sha1(key.getBytes()) : null;
					return consumer
							.transformWith(FrameSigner.create(keys.getPrivKey(), checkpointPosStrategy, filename, offset + skip, startingDigest, simKeyHash))
							.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset + skip))
							.peek(buf -> size[0] += buf.readRemaining())
							.transformWith(ChannelByteRanger.drop(skip));
				});
	}

	public Promise<ChannelConsumer<ByteBuf>> upload(KeyPair keys, String filename, long offset, @Nullable SimKey key) {
		// cut off the part of the file that is already there
		return node.getMetadata(keys.getPubKey(), filename)
				.thenComposeEx((signedCheckpoint, e) -> {
					if (e == NO_CHECKPOINT) {
						return offset == -1 || offset == 0 ?
								doUpload(keys, filename, 0, key, 0, new SHA256Digest()) :
								Promise.ofException(UPLOAD_OFFSET_EXCEEDS_FILE_SIZE);
					}
					if (e != null) {
						return Promise.ofException(e);
					}
					GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
					if (checkpoint.isTombstone()) {
						return Promise.ofException(UPLOADING_TO_TOMBSTONE);
					}
					if (offset == -1) {
						return Promise.ofException(FILE_ALREADY_EXISTS);
					}
					long size = checkpoint.getPosition();
					if (offset > size) {
						return Promise.ofException(UPLOAD_OFFSET_EXCEEDS_FILE_SIZE);
					}
					long skip = size - offset;
					if (!Objects.equals(checkpoint.getSimKeyHash(), key != null ? Hash.sha1(key.getBytes()) : null)) {
						return Promise.ofException(FILE_APPEND_WITH_OTHER_KEY);
					}
					return doUpload(keys, filename, offset, key, skip, checkpoint.getDigest());
				})
				.whenComplete(toLogger(logger, INFO, INFO, "upload", filename, offset, key, this));
	}

	public Promise<ChannelSupplier<ByteBuf>> download(PubKey space, String filename, long offset, long limit) {
		return node.getMetadata(space, filename)
				.thenComposeEx((signedMetadata, e) -> {
					if (e != null) {
						return Promise.ofException(e == NO_CHECKPOINT ? FILE_NOT_FOUND : e);
					}
					GlobalFsCheckpoint metadata = signedMetadata.getValue();
					if (metadata.isTombstone()) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					return node.download(space, filename, offset, limit)
							.thenApply(supplier -> supplier.transformWith(FrameVerifier.create(space, filename, offset, limit)));
				})
				.whenComplete(toLogger(logger, INFO, INFO, "download", filename, offset, limit, this));
	}

	public Promise<List<GlobalFsCheckpoint>> list(PubKey space, String glob) {
		return node.list(space, glob)
				.thenApply(list -> list.stream().map(SignedData::getValue).collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this));
	}

	public Promise<@Nullable GlobalFsCheckpoint> getMetadata(PubKey space, String filename) {
		return node.getMetadata(space, filename)
				.thenComposeEx((signedCheckpoint, e) ->
						e == null ?
								Promise.of(signedCheckpoint.getValue()) :
								e == NO_CHECKPOINT ?
										Promise.of(null) :
										Promise.ofException(e))
				.whenComplete(toLogger(logger, TRACE, "getMetadata", filename, this));
	}

	public Promise<Void> delete(KeyPair keys, String filename) {
		return node.delete(keys.getPubKey(), SignedData.sign(CHECKPOINT_CODEC, GlobalFsCheckpoint.createTombstone(filename), keys.getPrivKey()))
				.whenComplete(toLogger(logger, TRACE, "delete", filename, this));
	}

	public Promise<Void> deleteBulk(KeyPair keys, String glob) {
		return isWildcard(glob) ?
				node.list(keys.getPubKey(), glob)
						.thenCompose(list ->
								Promises.all(list.stream()
										.filter(signedMeta -> !signedMeta.getValue().isTombstone())
										.map(signedMeta -> delete(keys, signedMeta.getValue().getFilename()))))
						.whenComplete(toLogger(logger, TRACE, "deleteBulk", glob, this)) :
				delete(keys, glob);
	}
}
