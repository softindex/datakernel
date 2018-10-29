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

package io.global.fs.api;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.RepoID;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.file.FileUtils.escapeGlob;
import static java.util.stream.Collectors.toList;

public interface GlobalFsGateway {
	Promise<SerialConsumer<ByteBuf>> upload(GlobalPath path, long offset);

	default Promise<SerialConsumer<ByteBuf>> upload(GlobalPath path) {
		return upload(path, -1);
	}

	default SerialConsumer<ByteBuf> uploader(GlobalPath path, long offset) {
		return SerialConsumer.ofPromise(upload(path, offset));
	}

	default SerialConsumer<ByteBuf> uploader(GlobalPath path) {
		return SerialConsumer.ofPromise(upload(path, -1));
	}

	Promise<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset, long limit);

	default Promise<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset) {
		return download(path, offset, -1);
	}

	default Promise<SerialSupplier<ByteBuf>> download(GlobalPath path) {
		return download(path, 0, -1);
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path, long offset, long limit) {
		return SerialSupplier.ofPromise(download(path, offset, limit));
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path, long offset) {
		return SerialSupplier.ofPromise(download(path, offset, -1));
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path) {
		return SerialSupplier.ofPromise(download(path, 0, -1));
	}

	Promise<List<GlobalFsMetadata>> list(RepoID space, String glob);

	default Promise<GlobalFsMetadata> getMetadata(GlobalPath path) {
		return list(path.toRepoID(), escapeGlob(path.getPath()))
				.thenApply(list -> list.isEmpty() ? null : list.get(0));
	}

	Promise<Void> delete(GlobalPath path);

	Promise<Void> delete(RepoID space, String glob);

	default FsClient createFsAdapter(RepoID repo, CurrentTimeProvider timeProvider) {
		return new FsClient() {
			@Override
			public Promise<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
				return GlobalFsGateway.this.upload(GlobalPath.of(repo, filename), offset);
			}

			@Override
			public Promise<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
				return GlobalFsGateway.this.download(GlobalPath.of(repo, filename), offset, length);
			}

			@Override
			public Promise<Set<String>> move(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file moving in GlobalFS yet");
			}

			@Override
			public Promise<Set<String>> copy(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file copying in GlobalFS yet");
			}

			@Override
			public Promise<List<FileMetadata>> list(String glob) {
				return GlobalFsGateway.this.list(repo, glob)
						.thenApply(res -> res.stream()
								.map(meta -> new FileMetadata(meta.getLocalPath().getPath(), meta.getSize(), meta.getRevision()))
								.collect(toList()));
			}

			@Override
			public Promise<Void> delete(String glob) {
				return GlobalFsGateway.this.delete(repo, glob);
			}
		};
	}

	default FsClient createFsAdapter(RepoID space) {
		return createFsAdapter(space, CurrentTimeProvider.ofSystem());
	}
}
