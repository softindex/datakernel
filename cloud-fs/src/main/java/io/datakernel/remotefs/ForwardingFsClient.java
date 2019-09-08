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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class ForwardingFsClient implements FsClient {
	private final FsClient peer;

	public ForwardingFsClient(FsClient peer) {
		this.peer = peer;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset) {
		return peer.upload(name, offset);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		return peer.upload(name);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision) {
		return peer.upload(name, offset, revision);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long length) {
		return peer.download(name, offset, length);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset) {
		return peer.download(name, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name) {
		return peer.download(name);
	}

	@Override
	public Promise<Void> delete(String name) {
		return peer.delete(name);
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return peer.delete(name, revision);
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		return peer.copy(name, target);
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		return peer.copy(name, target, targetRevision);
	}

	@Override
	public Promise<Void> move(String name, String target) {
		return peer.move(name, target);
	}

	@Override
	public Promise<Void> move(String filename, String target, long targetRevision, long tombstoneRevision) {
		return peer.move(filename, target, targetRevision, tombstoneRevision);
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(String glob) {
		return peer.listEntities(glob);
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return peer.list(glob);
	}

	@Override
	public Promise<@Nullable FileMetadata> getMetadata(String name) {
		return peer.getMetadata(name);
	}

	@Override
	public Promise<Void> ping() {
		return peer.ping();
	}

	@Override
	public FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return peer.transform(into, from, globInto);
	}

	@Override
	public FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return peer.transform(into, from);
	}

	@Override
	public FsClient addingPrefix(String prefix) {
		return peer.addingPrefix(prefix);
	}

	@Override
	public FsClient subfolder(String folder) {
		return peer.subfolder(folder);
	}

	@Override
	public FsClient strippingPrefix(String prefix) {
		return peer.strippingPrefix(prefix);
	}

	@Override
	public FsClient filter(Predicate<String> predicate) {
		return peer.filter(predicate);
	}

	@Override
	public FsClient mount(String mountpoint, FsClient client) {
		return peer.mount(mountpoint, client);
	}
}
