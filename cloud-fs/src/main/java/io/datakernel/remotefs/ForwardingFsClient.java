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
import io.datakernel.csp.ChannelSupplier;

import java.util.List;
import java.util.Map;

public abstract class ForwardingFsClient implements FsClient {
	private final FsClient peer;

	public ForwardingFsClient(FsClient peer) {
		this.peer = peer;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		return peer.upload(filename, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return peer.download(filename, offset, length);
	}

	@Override
	public Promise<Void> copy(String filename, String newFilename) {
		return peer.copy(filename, newFilename);
	}

	@Override
	public Promise<Void> move(String filename, String newFilename) {
		return peer.move(filename, newFilename);
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return peer.moveBulk(changes);
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return peer.copyBulk(changes);
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return peer.list(glob);
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return peer.deleteBulk(glob);
	}

	@Override
	public Promise<Void> delete(String filename) {
		return peer.delete(filename);
	}
}
