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

package io.global.kv.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public interface GlobalKvNode {
	Promise<ChannelConsumer<SignedData<RawKvItem>>> upload(PubKey space, String table);

	Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String table, long timestamp);

	default Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String table) {
		return download(space, table, 0);
	}

	// have to have quick random access
	Promise<@Nullable SignedData<RawKvItem>> get(PubKey space, String table, byte[] key);

	// could be some optimized single put
	default Promise<Void> put(PubKey space, String table, SignedData<RawKvItem> item) {
		return ChannelSupplier.of(item).streamTo(ChannelConsumer.ofPromise(upload(space, table)));
	}

	Promise<Set<String>> list(PubKey space);
}
