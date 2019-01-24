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

import static io.datakernel.util.FileUtils.isWildcard;

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
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		return findMount(filename).upload(filename, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return findMount(filename).download(filename, offset, length);
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return Promises.toList(Stream.concat(Stream.of(root), mounts.values().stream()).map(f -> f.list(glob)))
				.thenApply(listOfLists -> FileMetadata.flatten(listOfLists.stream()));
	}

	@Override
	public Promise<Void> move(String filename, String newFilename) {
		FsClient first = findMount(filename);
		FsClient second = findMount(newFilename);
		if (first == second) {
			return first.move(filename, newFilename);
		}
		return first.download(filename)
				.thenCompose(supplier ->
						second.upload(filename)
								.thenCompose(supplier::streamTo))
				.thenCompose($ -> first.delete(filename));
	}

	@Override
	public Promise<Void> copy(String filename, String newFilename) {
		FsClient first = findMount(filename);
		FsClient second = findMount(newFilename);
		if (first == second) {
			return first.copy(filename, newFilename);
		}
		return first.download(filename)
				.thenCompose(supplier ->
						second.upload(filename)
								.thenCompose(supplier::streamTo));
	}

	@Override
	public Promise<Void> delete(String filename) {
		return findMount(filename).delete(filename);
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		if (!isWildcard(glob)) {
			return delete(glob);
		}
		return list(glob).thenCompose(list -> Promises.all(list.stream().map(meta -> delete(meta.getFilename()))));
	}

	@Override
	public FsClient mount(String mountpoint, FsClient client) {
		Map<String, FsClient> map = new HashMap<>(mounts);
		map.put(mountpoint, client.strippingPrefix(mountpoint + '/'));
		return new MountingFsClient(root, map);
	}
}
