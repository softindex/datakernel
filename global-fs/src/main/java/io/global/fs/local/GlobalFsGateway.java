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

package io.global.fs.local;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.exception.ConstantException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.List;
import java.util.Map;

import static io.datakernel.file.FileUtils.isWildcard;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static io.global.fs.api.GlobalFsNode.FILE_ALREADY_EXISTS;
import static io.global.fs.api.GlobalFsNode.UPLOADING_TO_TOMBSTONE;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class GlobalFsGateway implements FsClient, Initializable<GlobalFsGateway> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsGateway.class);

	public static final ConstantException UPLOAD_OFFSET_EXCEEDS_FILE_SIZE = new ConstantException(GlobalFsGateway.class, "Trying to upload at offset greater than known file size");

	private static final StructuredCodec<GlobalFsCheckpoint> METADATA_CODEC = REGISTRY.get(GlobalFsCheckpoint.class);

	private final GlobalFsDriver driver;

	private final GlobalFsNode node;
	private final PubKey space;
	private final PrivKey privKey;

	private final CheckpointPosStrategy checkpointPosStrategy;

	GlobalFsGateway(GlobalFsDriver driver, GlobalFsNode node, PubKey space, PrivKey privKey, CheckpointPosStrategy checkpointPosStrategy) {
		this.driver = driver;
		this.node = node;
		this.space = space;
		this.privKey = privKey;
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String filename, @Nullable GlobalFsCheckpoint metadata, long offset, long skip, SHA256Digest startingDigest) {
		long[] size = {offset + skip};
		Promise<SimKey> simKey = metadata != null ?
				driver.getPrivateKeyStorage().getKey(space, metadata.getSimKeyHash()) :
				Promise.of(driver.getPrivateKeyStorage().getCurrentSimKey());
		return simKey.thenCompose(key ->
				node.upload(space, filename, offset + skip)
						.thenApply(consumer -> {
							Hash simKeyHash = key != null ? Hash.sha1(key.getBytes()) : null;
							return consumer
									.transformWith(FrameSigner.create(privKey, checkpointPosStrategy, filename, offset + skip, startingDigest, simKeyHash))
									.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset + skip))
									.peek(buf -> size[0] += buf.readRemaining())
									.transformWith(ChannelByteRanger.drop(skip));
						}));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		// cut off the part of the file that is already there
		return node.getMetadata(space, filename)
				.thenComposeEx((signedCheckpoint, e) -> {
					if (e != null && e != NO_CHECKPOINT) {
						return Promise.ofException(e);
					}
					if (signedCheckpoint == null) {
						return offset == -1 || offset == 0 ?
								doUpload(filename, null, 0, 0, new SHA256Digest()) :
								Promise.ofException(UPLOAD_OFFSET_EXCEEDS_FILE_SIZE);
					}
					if (signedCheckpoint.getValue().isTombstone()) {
						return Promise.ofException(UPLOADING_TO_TOMBSTONE);
					}
					if (offset == -1) {
						return Promise.ofException(FILE_ALREADY_EXISTS);
					}
					GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
					long metaSize = checkpoint.getPosition();
					if (offset > metaSize) {
						return Promise.ofException(UPLOAD_OFFSET_EXCEEDS_FILE_SIZE);
					}
					long skip = metaSize - offset;
					return doUpload(filename, checkpoint, offset, skip, checkpoint.getDigest());
				})
				.whenComplete(toLogger(logger, "upload", filename, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long limit) {
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
							.thenCompose(supplier ->
									driver.getPrivateKeyStorage()
											.getKey(space, metadata.getSimKeyHash())
											.thenApply(key -> supplier
													.transformWith(FrameVerifier.create(space, filename, offset, limit))
													.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset))));
				})
				.whenComplete(toLogger(logger, "download", filename, offset, limit, this));
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return node.list(space, glob)
				.thenApply(res -> res.stream()
						.map(signedMeta -> {
							GlobalFsCheckpoint value = signedMeta.getValue();
							return new FileMetadata(value.getFilename(), value.isTombstone() ? -1 : value.getPosition(), 0);
						})
						.collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this));
	}

	@Override
	public Promise<FileMetadata> getMetadata(String filename) {
		return node.getMetadata(space, filename)
				.thenComposeEx((signedMeta, e) -> {
					if (e != null) {
						return e == NO_CHECKPOINT ? Promise.of(null) : Promise.ofException(e);
					}
					GlobalFsCheckpoint value = signedMeta.getValue();
					return Promise.of(new FileMetadata(value.getFilename(), value.isTombstone() ? -1 : value.getPosition(), 0));
				})
				.whenComplete(toLogger(logger, TRACE, "getMetadata", filename, this));
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return isWildcard(glob) ?
				node.list(space, glob)
						.thenCompose(list ->
								Promises.all(list.stream()
										.filter(signedMeta -> !signedMeta.getValue().isTombstone())
										.map(signedMeta -> delete(signedMeta.getValue().getFilename()))))
						.whenComplete(toLogger(logger, TRACE, "deleteBulk", glob, this)) :
				delete(glob);
	}

	@Override
	public Promise<Void> delete(String filename) {
		return node.delete(space, SignedData.sign(METADATA_CODEC, GlobalFsCheckpoint.createTombstone(filename), privKey))
				.whenComplete(toLogger(logger, TRACE, "delete", filename, this));
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file moving in GlobalFS yet");
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file copying in GlobalFS yet");
	}
}
