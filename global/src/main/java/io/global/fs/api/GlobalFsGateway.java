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

import io.datakernel.async.Stage;
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
	Stage<SerialConsumer<ByteBuf>> upload(GlobalPath path, long offset);

	default Stage<SerialConsumer<ByteBuf>> upload(GlobalPath path) {
		return upload(path, -1);
	}

	default SerialConsumer<ByteBuf> uploader(GlobalPath path, long offset) {
		return SerialConsumer.ofStage(upload(path, offset));
	}

	default SerialConsumer<ByteBuf> uploader(GlobalPath path) {
		return SerialConsumer.ofStage(upload(path, -1));
	}

	Stage<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset, long limit);

	default Stage<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset) {
		return download(path, offset, -1);
	}

	default Stage<SerialSupplier<ByteBuf>> download(GlobalPath path) {
		return download(path, 0, -1);
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path, long offset, long limit) {
		return SerialSupplier.ofStage(download(path, offset, limit));
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path, long offset) {
		return SerialSupplier.ofStage(download(path, offset, -1));
	}

	default SerialSupplier<ByteBuf> downloader(GlobalPath path) {
		return SerialSupplier.ofStage(download(path, 0, -1));
	}

	Stage<List<GlobalFsMetadata>> list(RepoID space, String glob);

	default Stage<GlobalFsMetadata> getMetadata(GlobalPath path) {
		return list(path.toRepoID(), escapeGlob(path.getPath()))
				.thenApply(list -> list.isEmpty() ? null : list.get(0));
	}

	Stage<Void> delete(GlobalPath path);

	Stage<Void> delete(RepoID space, String glob);

	default FsClient createFsAdapter(RepoID space, CurrentTimeProvider timeProvider) {
		return new FsClient() {
			@Override
			public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
				return GlobalFsGateway.this.upload(GlobalPath.of(space, filename), offset);
			}

			@Override
			public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
				return GlobalFsGateway.this.download(GlobalPath.of(space, filename), offset, length);
			}

			@Override
			public Stage<Set<String>> move(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file moving in GlobalFS yet");
			}

			@Override
			public Stage<Set<String>> copy(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file copying in GlobalFS yet");
			}

			@Override
			public Stage<List<FileMetadata>> list(String glob) {
				return GlobalFsGateway.this.list(space, glob)
						.thenApply(res -> res.stream()
								.map(meta -> new FileMetadata(meta.getLocalPath().getPath(), meta.getSize(), meta.getRevision()))
								.collect(toList()));
			}

			@Override
			public Stage<Void> delete(String glob) {
				return GlobalFsGateway.this.delete(space, glob);
			}
		};
	}

	default FsClient createFsAdapter(RepoID space) {
		return createFsAdapter(space, CurrentTimeProvider.ofSystem());
	}
}
