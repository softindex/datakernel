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
import io.datakernel.csp.ChannelSupplier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class MountingFsClient implements FsClient {
	private final FsClient root;
	private final Map<String, FsClient> mounts;

	MountingFsClient(FsClient root, Map<String, FsClient> mounts) {
		this.root = root;
		this.mounts = mounts;
	}

	private FsClient findMount(String filename) {
		int idx = filename.lastIndexOf('/');
		while (idx != -1) {
			String path = filename.substring(0, idx);
			FsClient mount = mounts.get(path);
			if (mount != null) {
				return mount;
			}
			idx = filename.lastIndexOf('/', idx - 1);
		}
		return root;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision) {
		return findMount(name).upload(name, offset, revision);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long length) {
		return findMount(name).download(name, offset, length);
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(String glob) {
		return Promises.toList(Stream.concat(Stream.of(root), mounts.values().stream()).map(f -> f.listEntities(glob)))
				.thenApply(listOfLists -> FileMetadata.flatten(listOfLists.stream()));
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return Promises.toList(Stream.concat(Stream.of(root), mounts.values().stream()).map(f -> f.list(glob)))
				.thenApply(listOfLists -> FileMetadata.flatten(listOfLists.stream()));
	}

	@Override
	public Promise<Void> move(String name, String target, long targetRevision, long removeRevision) {
		FsClient first = findMount(name);
		FsClient second = findMount(target);
		if (first == second) {
			return first.move(name, target, targetRevision, removeRevision);
		}
		return first.download(name)
				.thenCompose(supplier ->
						second.upload(name, 0, targetRevision)
								.thenCompose(supplier::streamTo))
				.thenCompose($ -> first.delete(name));
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		FsClient first = findMount(name);
		FsClient second = findMount(target);
		if (first == second) {
			return first.copy(name, target, targetRevision);
		}
		return first.download(name)
				.thenCompose(supplier ->
						second.upload(name, 0, targetRevision)
								.thenCompose(supplier::streamTo));
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return findMount(name).delete(name, revision);
	}

	@Override
	public FsClient mount(String mountpoint, FsClient client) {
		Map<String, FsClient> map = new HashMap<>(mounts);
		map.put(mountpoint, client.strippingPrefix(mountpoint + '/'));
		return new MountingFsClient(root, map);
	}
}
