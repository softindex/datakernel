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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.MetadataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.file.FileUtils.escapeGlob;
import static java.util.stream.Collectors.toList;

public class RemoteFsMetadataStorage implements MetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsMetadataStorage.class);

	private final FsClient fsClient;

	public RemoteFsMetadataStorage(FsClient fsClient) {
		this.fsClient = fsClient;
	}

	@Override
	public Promise<Void> store(SignedData<GlobalFsMetadata> signedMetadata) {
		logger.trace("pushing {}", signedMetadata);
		String path = signedMetadata.getData().getLocalPath().getPath();
		return fsClient.delete(escapeGlob(path))
				.thenCompose($ -> fsClient.upload(path, 0)) // offset 0 because atst this same file could be fetched from another node too
				.thenCompose(SerialSupplier.of(ByteBuf.wrapForReading(signedMetadata.toBytes()))::streamTo);
	}

	@Override
	public Promise<SignedData<GlobalFsMetadata>> load(String fileName) {
		logger.trace("loading {}", fileName);
		return fsClient.getMetadata(fileName)
				.thenCompose(metameta -> {
					if (metameta == null) {
						return Promise.of(null);
					}
					return fsClient.download(fileName)
							.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
							.thenCompose(buf -> {
								try {
									return Promise.of(SignedData.ofBytes(buf.asArray(), GlobalFsMetadata::fromBytes));
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
							});
				});
	}


	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(String glob) {
		return fsClient.list(glob)
				.thenCompose(res -> Promises.collectSequence(toList(), res.stream().map(metameta -> load(metameta.getName()))));
	}
}
