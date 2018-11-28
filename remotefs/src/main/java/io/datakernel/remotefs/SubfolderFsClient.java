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

package io.datakernel.remotefs;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;

import java.io.File;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

class SubfolderFsClient implements FsClient {

	private final FsClient parent;
	private final String folder;

	SubfolderFsClient(FsClient parent, String folder) {
		this.parent = parent;
		this.folder = folder.endsWith(File.separator) ? folder : folder + File.separator;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		return parent.upload(folder + filename, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return parent.download(folder + filename, offset, length);
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return parent.moveBulk(changes);
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return parent.copyBulk(changes);
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return parent.list(folder + glob)
				.thenApply(list ->
						list.stream()
								.map(meta -> new FileMetadata(
										meta.getFilename().substring(folder.length()),
										meta.getSize(),
										meta.getTimestamp()))
								.collect(toList()));
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return parent.deleteBulk(folder + glob);
	}

	@Override
	public FsClient subfolder(String folder) {
		return new SubfolderFsClient(parent, this.folder + folder);
	}
}
