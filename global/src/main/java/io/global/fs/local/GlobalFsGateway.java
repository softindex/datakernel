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
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.List;
import java.util.Map;

import static io.datakernel.file.FileUtils.isWildcard;
import static io.global.fs.api.GlobalFsNode.CANT_VERIFY_METADATA;
import static io.global.fs.api.MetadataStorage.NO_METADATA;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class GlobalFsGateway implements FsClient, Initializable<GlobalFsGateway> {
	private static final StacklessException CHECKPOINT_SIG = new StacklessException(GlobalFsGateway.class, "Received checkpoint signature is not verified");

	private static final StructuredCodec<GlobalFsMetadata> METADATA_CODEC = REGISTRY.get(GlobalFsMetadata.class);

	private final GlobalFsDriver driver;

	private final GlobalFsNode node;
	private final PubKey pubKey;
	private final PrivKey privKey;

	private final CheckpointPosStrategy checkpointPosStrategy;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	GlobalFsGateway(GlobalFsDriver driver, GlobalFsNode node, PubKey pubKey, PrivKey privKey, CheckpointPosStrategy checkpointPosStrategy) {
		this.driver = driver;
		this.node = node;
		this.pubKey = pubKey;
		this.privKey = privKey;
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String filename, @Nullable GlobalFsMetadata metadata, long offset, long skip, SHA256Digest startingDigest) {
		long[] size = {offset + skip};
		Promise<SimKey> simKey = metadata != null ?
				driver.getPrivateKeyStorage().getKey(pubKey, metadata.getSimKeyHash()) :
				Promise.of(driver.getPrivateKeyStorage().getCurrentSimKey());
		return simKey.thenCompose(key ->
				node.upload(pubKey, filename, offset + skip)
						.thenApply(consumer -> consumer
								.transformWith(FrameSigner.create(privKey, checkpointPosStrategy, filename, offset + skip, startingDigest))
								.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset + skip))
								.peek(buf -> size[0] += buf.readRemaining())
								.transformWith(ChannelByteRanger.drop(skip))
								.withAcknowledgement(ack -> ack
										.thenCompose($ -> {
											GlobalFsMetadata updatedMetadata =
													GlobalFsMetadata.of(filename, size[0], now.currentTimeMillis(), key != null ? Hash.sha1(key.getBytes()) : null);
											return node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, updatedMetadata, privKey));
										}))));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		// cut off the part of the file that is already there
		return node.getMetadata(pubKey, filename)
				.thenComposeEx((signedMetadata, e) -> {
					if (e != null && e != NO_METADATA) {
						return Promise.ofException(e);
					}
					if (signedMetadata != null && !signedMetadata.verify(pubKey)) {
						return Promise.ofException(CANT_VERIFY_METADATA);
					}
					if (signedMetadata == null || signedMetadata.getValue().isRemoved()) {
						return offset == -1 || offset == 0 ?
								doUpload(filename, null, 0, 0, new SHA256Digest()) :
								Promise.ofException(new StacklessException(GlobalFsGateway.class, "Trying to upload at offset greater than known file size"));
					}
					if (offset == -1) {
						return Promise.ofException(new StacklessException(GlobalFsGateway.class, "File already exists"));
					}
					GlobalFsMetadata metadata = signedMetadata.getValue();
					long metaSize = metadata.getSize();
					if (offset > metaSize) {
						return Promise.ofException(new StacklessException(GlobalFsGateway.class, "Trying to upload at offset greater than the file size"));
					}
					long skip = metaSize - offset;
					return node.download(pubKey, filename, metaSize, 0)
							.thenCompose(supplier -> supplier.toCollector(toList()))
							.thenCompose(frames -> {
								if (frames.size() != 1) {
									return Promise.ofException(new StacklessException(GlobalFsGateway.class, "No checkpoint at metadata size position!"));
								}
								SignedData<GlobalFsCheckpoint> checkpoint = frames.get(0).getCheckpoint();
								if (!checkpoint.verify(pubKey)) {
									return Promise.ofException(CHECKPOINT_SIG);
								}
								return doUpload(filename, metadata, offset, skip, checkpoint.getValue().getDigest());
							});
				});
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long limit) {
		return node.getMetadata(pubKey, filename)
				.thenComposeEx((signedMetadata, e) -> {
					if (e != null) {
						return Promise.ofException(e == NO_METADATA ? FILE_NOT_FOUND : e);
					}
					if (!signedMetadata.verify(pubKey)) {
						return Promise.ofException(CANT_VERIFY_METADATA);
					}
					GlobalFsMetadata metadata = signedMetadata.getValue();
					if (metadata.isRemoved()) {
						return Promise.ofException(FILE_NOT_FOUND);
					}
					return node.download(pubKey, filename, offset, limit)
							.thenCompose(supplier ->
									driver.getPrivateKeyStorage()
											.getKey(pubKey, metadata.getSimKeyHash())
											.thenApply(key -> supplier
													.transformWith(FrameVerifier.create(pubKey, filename, offset, limit))
													.transformWith(CipherTransformer.create(key, CryptoUtils.nonceFromString(filename), offset))));
				});
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return node.list(pubKey, glob)
				.thenApply(res -> res.stream()
						.filter(signedMeta -> signedMeta.verify(pubKey))
						.map(signedMeta -> signedMeta.getValue().toFileMetadata())
						.collect(toList()));
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return isWildcard(glob) ?
				node.list(pubKey, glob)
						.thenCompose(list ->
								Promises.all(list.stream()
										.filter(signedMeta -> !signedMeta.getValue().isRemoved() && signedMeta.verify(pubKey))
										.map(signedMeta -> {
											GlobalFsMetadata removed = signedMeta.getValue().toRemoved(now.currentTimeMillis());
											return node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, removed, privKey));
										}))) :
				node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, GlobalFsMetadata.ofRemoved(glob, now.currentTimeMillis()), privKey));
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
