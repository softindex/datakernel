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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.SerialByteBufCutter;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public final class GlobalFsGatewayAdapter implements FsClient, Initializable<GlobalFsGatewayAdapter> {
	private static final StacklessException METADATA_SIG = new StacklessException(GlobalFsGatewayAdapter.class, "Received metadata signature is not verified");
	private static final StacklessException CHECKPOINT_SIG = new StacklessException(GlobalFsGatewayAdapter.class, "Received checkpoint signature is not verified");

	private final GlobalFsDriver driver;

	private final GlobalFsNode node;
	private final PubKey pubKey;
	private final PrivKey privKey;

	private final CheckpointPosStrategy checkpointPosStrategy;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	GlobalFsGatewayAdapter(GlobalFsDriver driver, GlobalFsNode node,
			PubKey pubKey, PrivKey privKey,
			CheckpointPosStrategy checkpointPosStrategy) {
		this.driver = driver;
		this.node = node;
		this.pubKey = pubKey;
		this.privKey = privKey;
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		long normalizedOffset = offset == -1 ? 0 : offset;
		long[] size = {normalizedOffset};
		long[] toSkip = {0};
		SHA256Digest[] digest = new SHA256Digest[1];

		// all the below cutting logic is also present on the server
		// but we simply skip the prefix to not (potentially) send it over the network
		// and also getting the proper digest for checkpoints ofc

		return node.getMetadata(pubKey, filename)
				.thenCompose(signedMeta -> {
					if (signedMeta == null) {
						digest[0] = new SHA256Digest();
						return Promise.complete();
					}
					if (!signedMeta.verify(pubKey)) {
						return Promise.ofException(METADATA_SIG);
					}

					GlobalFsMetadata meta = signedMeta.getData();
					long metaSize = meta.getSize();
					if (offset > metaSize) {
						return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "Trying to upload at offset greater than the file size"));
					}
					toSkip[0] = metaSize - offset;
					return node.download(pubKey, filename, metaSize, 0)
							.thenCompose(supplier -> supplier.toCollector(toList()))
							.thenCompose(frames -> {
								if (frames.size() != 1) {
									return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "No checkpoint at metadata size position!"));
								}
								SignedData<GlobalFsCheckpoint> checkpoint = frames.get(0).getCheckpoint();
								if (!checkpoint.verify(pubKey)) {
									return Promise.ofException(CHECKPOINT_SIG);
								}
								digest[0] = checkpoint.getData().getDigest();
								return Promise.complete();
							});
				})
				.thenCompose($ ->
						node.upload(pubKey, filename, offset != -1 ? offset + toSkip[0] : offset)
								.thenApply(consumer -> {
									return consumer
											.apply(FrameSigner.create(privKey, checkpointPosStrategy, filename, normalizedOffset + toSkip[0], digest[0]))
											.apply(SerialByteBufCutter.create(toSkip[0]))
											.peek(buf -> size[0] += buf.readRemaining())
											.withAcknowledgement(ack -> ack
													.thenCompose($2 -> {
														GlobalFsMetadata meta = GlobalFsMetadata.of(filename, size[0], now.currentTimeMillis());
														return node.pushMetadata(pubKey, SignedData.sign(meta, privKey));
													}));
								}));
	}

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(String filename, long offset, long limit) {
		return node.download(pubKey, filename, offset, limit)
				.thenApply(supplier -> supplier.apply(FrameVerifier.create(pubKey, filename, offset, limit)));
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return node.list(pubKey, glob)
				.thenApply(res -> res.stream()
						.filter(signedMeta -> signedMeta.verify(pubKey))
						.map(signedMeta -> signedMeta.getData().toFileMetadata())
						.collect(toList()));
	}

	@Override
	public Promise<Void> delete(String glob) {
		return node.list(pubKey, glob)
				.thenCompose(list ->
						Promises.all(list.stream()
								.filter(signedMeta -> signedMeta.verify(pubKey))
								.map(signedMeta ->
										node.pushMetadata(pubKey, SignedData.sign(signedMeta.getData().toRemoved(), privKey)))));
	}

	@Override
	public Promise<Set<String>> move(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file moving in GlobalFS yet");
	}

	@Override
	public Promise<Set<String>> copy(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file copying in GlobalFS yet");
	}
}
