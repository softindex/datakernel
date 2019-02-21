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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

public final class EmptyFsClient implements FsClient {
	public static final EmptyFsClient INSTANCE = new EmptyFsClient();

	private EmptyFsClient() {
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision) {
		return Promise.of(ChannelConsumers.recycling());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long length) {
		return Promise.ofException(FILE_NOT_FOUND);
	}

	@Override
	public Promise<Void> move(String filename, String target, long targetRevision, long removeRevision) {
		return Promise.complete();
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		return Promise.complete();
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(String glob) {
		return Promise.of(emptyList());
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return Promise.of(emptyList());
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return Promise.complete();
	}

	@Override
	public FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return this;
	}

	@Override
	public FsClient strippingPrefix(String prefix) {
		return this;
	}

	@Override
	public FsClient filter(Predicate<String> predicate) {
		return this;
	}
}
