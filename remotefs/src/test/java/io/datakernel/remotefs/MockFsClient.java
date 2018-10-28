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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockFsClient implements FsClient {

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		if (offset == -1) {
			return Promise.ofException(new RemoteFsException(MockFsClient.class, "FileAlreadyExistsException"));
		}
		return Promise.of(SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle)));
	}

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return Promise.of(SerialSupplier.of(ByteBuf.wrapForReading("mock file".substring((int) offset, length == -1 ? 9 : (int) (offset + length)).getBytes(UTF_8))));
	}

	@Override
	public Promise<Set<String>> move(Map<String, String> changes) {
		return Promise.of(Collections.emptySet());
	}

	@Override
	public Promise<Set<String>> copy(Map<String, String> changes) {
		return Promise.of(Collections.emptySet());
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return Promise.of(Collections.emptyList());
	}

	@Override
	public Promise<Void> delete(String glob) {
		return Promise.ofException(new RemoteFsException(MockFsClient.class, "no files to delete"));
	}
}
