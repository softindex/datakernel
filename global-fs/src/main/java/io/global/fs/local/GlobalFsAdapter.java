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
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ConstantException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Initializable;
import io.global.common.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toList;

public final class GlobalFsAdapter implements FsClient, Initializable<GlobalFsAdapter> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsAdapter.class);

	public static final ConstantException UPLOAD_OFFSET_EXCEEDS_FILE_SIZE = new ConstantException(GlobalFsAdapter.class, "Trying to upload at offset greater than known file size");
	public static final ConstantException UPK_UPLOAD = new ConstantException(GlobalFsAdapter.class, "Trying to upload to public key without knowing it's private key");
	public static final ConstantException UPK_DELETE = new ConstantException(GlobalFsAdapter.class, "Trying to delete file at public key without knowing it's private key");

	private final GlobalFsDriver gateway;
	private final PubKey space;

	@Nullable
	private final PrivKey privKey;

	@Nullable
	private SimKey currentSimKey = null;

	public GlobalFsAdapter(GlobalFsDriver gateway, PubKey space, @Nullable PrivKey privKey) {
		this.gateway = gateway;
		this.space = space;
		this.privKey = privKey;
	}

	@Nullable
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public void setCurrentSimKey(@Nullable SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		return privKey != null ?
				gateway.upload(new KeyPair(privKey, space), filename, offset, currentSimKey) :
				Promise.ofException(UPK_UPLOAD);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long limit) {
		return gateway.download(space, filename, offset, limit)
				.thenApply(supplier -> supplier
						.transformWith(CipherTransformer.create(currentSimKey, CryptoUtils.nonceFromString(filename), offset)));
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return gateway.list(space, glob)
				.thenApply(res -> res.stream()
						.map(checkpoint -> new FileMetadata(
								checkpoint.getFilename(),
								checkpoint.isTombstone() ? -1 : checkpoint.getPosition(),
								0
						))
						.collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this));
	}

	@Override
	public Promise<FileMetadata> getMetadata(String filename) {
		return gateway.getMetadata(space, filename)
				.thenApply(checkpoint ->
						checkpoint != null ?
								new FileMetadata(checkpoint.getFilename(), checkpoint.isTombstone() ? -1 : checkpoint.getPosition(), 0) :
								null)
				.whenComplete(toLogger(logger, TRACE, "getMetadata", filename, this));
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return privKey != null ?
				gateway.delete(new KeyPair(privKey, space), glob) :
				Promise.ofException(UPK_DELETE);
	}

	@Override
	public Promise<Void> delete(String filename) {
		return privKey != null ?
				gateway.delete(new KeyPair(privKey, space), filename) :
				Promise.ofException(UPK_DELETE);
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
