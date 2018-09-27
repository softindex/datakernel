/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This component handles one of GlobalFS nodes.
 */
public interface GlobalFsNode {
	StacklessException RECURSIVE_ERROR = new StacklessException("Trying to download a file from a server that also tries to download this file.");
	StacklessException FETCH_DID_NOTHING = new StacklessException("Did not fetch anything from given node.");

	Stage<SerialSupplier<DataFrame>> download(GlobalFsPath file, long offset, long limit);

	default SerialSupplier<DataFrame> downloader(GlobalFsPath file, long offset, long limit) {
		return SerialSupplier.ofStage(download(file, offset, limit));
	}

	Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset);

	default SerialConsumer<DataFrame> uploader(GlobalFsPath file, long offset) {
		return SerialConsumer.ofStage(upload(file, offset));
	}

	Stage<List<GlobalFsMetadata>> list(GlobalFsName name, String glob);

	default Stage<GlobalFsMetadata> getMetadata(GlobalFsPath file) {
		return list(file.getGlobalFsName(), file.getPath()).thenApply(res -> res.size() == 1 ? res.get(0) : null);
	}

	Stage<Void> delete(GlobalFsName name, String glob);

	default Stage<Void> delete(GlobalFsPath file) {
		return delete(file.getGlobalFsName(), file.getPath());
	}

	Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes);

	Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes);

	default GlobalFsFileSystem getFileSystem(GlobalFsName name) {
		return new GlobalFsFileSystem() {
			@Override
			public GlobalFsName getName() {
				return name;
			}

			@Override
			public Stage<SerialConsumer<DataFrame>> upload(String file, long offset) {
				return GlobalFsNode.this.upload(name.addressOf(file), offset);
			}

			@Override
			public Stage<SerialSupplier<DataFrame>> download(String file, long offset, long length) {
				return GlobalFsNode.this.download(name.addressOf(file), offset, length);
			}

			@Override
			public Stage<List<GlobalFsMetadata>> list(String glob) {
				return GlobalFsNode.this.list(name, glob);
			}

			@Override
			public Stage<Void> delete(String glob) {
				return GlobalFsNode.this.delete(name, glob);
			}

			@Override
			public Stage<Set<String>> copy(Map<String, String> changes) {
				return GlobalFsNode.this.copy(name, changes);
			}

			@Override
			public Stage<Set<String>> move(Map<String, String> changes) {
				return GlobalFsNode.this.move(name, changes);
			}
		};
	}

	interface GlobalFsFileSystem {
		GlobalFsName getName();

		Stage<SerialConsumer<DataFrame>> upload(String file, long offset);

		default SerialConsumer<DataFrame> uploadSerial(String file, long offset) {
			return SerialConsumer.ofStage(upload(file, offset));
		}

		Stage<SerialSupplier<DataFrame>> download(String file, long offset, long length);

		default SerialSupplier<DataFrame> downloadSerial(String file, long offset, long length) {
			return SerialSupplier.ofStage(download(file, offset, length));
		}

		Stage<List<GlobalFsMetadata>> list(String glob);

		default Stage<GlobalFsMetadata> getMetadata(String file) {
			return list(file).thenApply(res -> res.size() == 1 ? res.get(0) : null);
		}

		Stage<Void> delete(String glob);

		Stage<Set<String>> copy(Map<String, String> changes);

		Stage<Set<String>> move(Map<String, String> changes);
	}
}
