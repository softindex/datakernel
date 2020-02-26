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

package io.global.crdt;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;

import java.util.Set;

public interface GlobalCrdtNode {
	Promise<StreamConsumer<SignedData<RawCrdtData>>> upload(PubKey space, String table);

	Promise<StreamSupplier<SignedData<RawCrdtData>>> download(PubKey space, String table, long revision);

	default Promise<StreamSupplier<SignedData<RawCrdtData>>> download(PubKey space, String table) {
		return download(space, table, 0);
	}

	Promise<StreamConsumer<SignedData<byte[]>>> remove(PubKey space, String table);

	Promise<Set<String>> list(PubKey space);
}
