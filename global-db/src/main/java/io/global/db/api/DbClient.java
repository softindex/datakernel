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

package io.global.db.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.db.DbItem;

import java.util.List;

public interface DbClient {
	Promise<ChannelConsumer<DbItem>> upload(String table);

	Promise<ChannelSupplier<DbItem>> download(String table, long timestamp);

	default Promise<ChannelSupplier<DbItem>> download(String table) {
		return download(table, 0);
	}

	Promise<ChannelConsumer<byte[]>> remove(String table);

	Promise<DbItem> get(String table, byte[] key);

	default Promise<Void> put(String table, DbItem item) {
		return ChannelSupplier.of(item).streamTo(ChannelConsumer.ofPromise(upload(table)));
	}

	default Promise<Void> remove(String table, byte[] key) {
		return ChannelSupplier.of(key).streamTo(ChannelConsumer.ofPromise(remove(table)));
	}

	Promise<List<String>> list();
}
