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
import io.datakernel.util.FileUtils;
import io.datakernel.util.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

final class TransformFsClient implements FsClient {
	private final FsClient parent;
	private final Function<String, Optional<String>> into;
	private final Function<String, Optional<String>> from;
	private final Function<String, Optional<String>> globInto;

	TransformFsClient(FsClient parent, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		this.parent = parent;
		this.into = into;
		this.from = from;
		this.globInto = globInto;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		Optional<String> transformed = into.apply(filename);
		if (!transformed.isPresent()) {
			return Promise.of(ChannelConsumers.recycling());
		}
		return parent.upload(transformed.get(), offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		Optional<String> transformed = into.apply(filename);
		if (!transformed.isPresent()) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		return parent.download(transformed.get(), offset, length);
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return parent.moveBulk(transformChanges(changes));
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return parent.copyBulk(transformChanges(changes));
	}

	private Map<String, String> transformChanges(Map<String, String> changes) {
		return changes.entrySet().stream()
				.map(e -> new Tuple2<>(into.apply(e.getKey()), into.apply(e.getValue())))
				.filter(t -> t.getValue1().isPresent() && t.getValue2().isPresent())
				.collect(Collectors.toMap(t -> t.getValue1().get(), t -> t.getValue2().get()));
	}

	@Override
	public Promise<Void> move(String filename, String newFilename) {
		return renamingOp(filename, newFilename, parent::move);
	}

	@Override
	public Promise<Void> copy(String filename, String newFilename) {
		return renamingOp(filename, newFilename, parent::copy);
	}

	private Promise<Void> renamingOp(String filename, String newFilename, BiFunction<String, String, Promise<Void>> original) {
		Optional<String> transformed = into.apply(filename);
		Optional<String> transformedNew = into.apply(newFilename);
		if (!transformed.isPresent() || !transformedNew.isPresent()) {
			return Promise.complete();
		}
		return original.apply(transformed.get(), transformedNew.get());
	}


	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		Predicate<String> pred = FileUtils.getGlobStringPredicate(glob);
		return parent.list(globInto.apply(glob).orElse("**"))
				.thenApply(list -> list.stream()
						.map(meta ->
								from.apply(meta.getFilename())
										.map(name -> new FileMetadata(name, meta.getSize(), meta.getTimestamp())))
						.filter(meta -> meta.isPresent() && pred.test(meta.get().getFilename()))
						.map(Optional::get)
						.collect(toList()));
	}

	@Override
	public Promise<FileMetadata> getMetadata(String filename) {
		Optional<String> transformed = into.apply(filename);
		return transformed.map(s ->
				parent.getMetadata(s)
						.thenApply(meta -> {
							if (meta == null) {
								return null;
							}
							return from.apply(meta.getFilename())
									.map(name -> new FileMetadata(name, meta.getSize(), meta.getTimestamp()))
									.orElse(null);
						})).orElse(Promise.of(null));
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
		Optional<String> transformed = into.apply(filename);
		if (!transformed.isPresent()) {
			return Promise.complete();
		}
		return parent.delete(transformed.get());
	}

	@Override
	public FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		if (into == this.from && from == this.into) { // huh
			return parent;
		}
		return new TransformFsClient(parent,
				name -> into.apply(name).flatMap(this.into),
				name -> this.from.apply(name).flatMap(from),
				name -> globInto.apply(name).flatMap(this.globInto)
		);
	}
}
