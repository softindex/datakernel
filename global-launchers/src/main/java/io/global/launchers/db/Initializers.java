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

package io.global.launchers.db;

import io.datakernel.config.Config;
import io.datakernel.util.Initializer;
import io.global.db.LocalGlobalDbNode;

import java.util.HashSet;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofList;
import static io.global.fs.local.LocalGlobalFsNode.DEFAULT_LATENCY_MARGIN;
import static io.global.launchers.GlobalConfigConverters.ofPubKey;
import static java.util.Collections.emptyList;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<LocalGlobalDbNode> ofLocalGlobalDbNode(Config config) {
		return node -> node
				.withManagedPubKeys(new HashSet<>(config.get(ofList(ofPubKey()), "managedKeys", emptyList())))
				.withLatencyMargin(config.get(ofDuration(), "latencyMargin", DEFAULT_LATENCY_MARGIN));
	}
}
