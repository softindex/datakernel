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

package io.datakernel.remotefs;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.datakernel.util.CollectorsEx.toMap;
import static java.util.stream.Collectors.toList;

final class FilterFsClient implements FsClient {
	private final FsClient parent;
	private final Predicate<String> predicate;

	FilterFsClient(FsClient parent, Predicate<String> predicate) {
		this.parent = parent;
		this.predicate = predicate;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		if (!predicate.test(filename)) {
			return Promise.of(ChannelConsumers.recycling());
		}
		return parent.upload(filename, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		if (!predicate.test(filename)) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		return parent.download(filename, offset, length);
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return parent.moveBulk(changes.entrySet().stream()
				.filter(e -> !predicate.test(e.getKey()) && !predicate.test(e.getValue()))
				.collect(toMap()));
	}

	@Override
	public Promise<Void> move(String filename, String newFilename) {
		if (!predicate.test(filename) || !predicate.test(newFilename)) {
			return Promise.complete();
		}
		return parent.move(filename, newFilename);
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return parent.copyBulk(changes.entrySet().stream()
				.filter(e -> !predicate.test(e.getKey()) && !predicate.test(e.getValue()))
				.collect(toMap()));
	}

	@Override
	public Promise<Void> copy(String filename, String newFilename) {
		if (!predicate.test(filename) || !predicate.test(newFilename)) {
			return Promise.complete();
		}
		return parent.copy(filename, newFilename);
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return parent.list(glob)
				.thenApply(list -> list.stream()
						.filter(meta -> predicate.test(meta.getFilename()))
						.collect(toList()));
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}


	@Override
	public Promise<Void> deleteBulk(String glob) {
		return list(glob) // use that list impl
				.thenCompose(list ->
						Promises.all(list.stream()
								.map(meta -> delete(meta.getFilename()))));
	}

	@Override
	public Promise<Void> delete(String filename) {
		if (!predicate.test(filename)) {
			return Promise.complete();
		}
		return parent.delete(filename);
	}
}
