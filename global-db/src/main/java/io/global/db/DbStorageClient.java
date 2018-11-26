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

package io.global.db;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.db.api.DbClient;
import io.global.db.api.DbStorage;

import java.util.List;

public final class DbStorageClient implements DbClient {
	private final DbStorage storage;

	public DbStorageClient(DbStorage storage) {
		this.storage = storage;
	}

	@Override
	public Promise<ChannelConsumer<DbItem>> upload(String table) {
		throw new UnsupportedOperationException("DbStorageClient#upload is not implemented yet");
	}

	@Override
	public Promise<ChannelSupplier<DbItem>> download(String table, long timestamp) {
		throw new UnsupportedOperationException("DbStorageClient#download is not implemented yet");
	}

	@Override
	public Promise<ChannelConsumer<byte[]>> remove(String table) {
		throw new UnsupportedOperationException("DbStorageClient#remove is not implemented yet");
	}

	@Override
	public Promise<DbItem> get(String table, byte[] key) {
		throw new UnsupportedOperationException("DbStorageClient#get is not implemented yet");
	}

	@Override
	public Promise<List<String>> list() {
		throw new UnsupportedOperationException("DbStorageClient#list is not implemented yet");
	}
}
