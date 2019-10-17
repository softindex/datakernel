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

package io.global.pm.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public interface GlobalPmNode {

	Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox);

	Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp);

	default Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox) {
		return download(space, mailBox, 0);
	}

	// accepts tombstones
	default Promise<Void> send(PubKey space, String mailBox, SignedData<RawMessage> message) {
		return ChannelSupplier.of(message).streamTo(ChannelConsumer.ofPromise(upload(space, mailBox)));
	}

	// does not return tombstones
	Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox);

	Promise<Set<String>> list(PubKey space);
}
