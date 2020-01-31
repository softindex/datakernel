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

package io.global.launchers.ot;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.global.ot.server.GlobalOTNodeImpl;

import static io.datakernel.config.ConfigConverters.ofBoolean;
import static io.datakernel.config.ConfigConverters.ofRetryPolicy;
import static io.global.ot.server.GlobalOTNodeImpl.DEFAULT_POLL_MASTER_REPOSITORIES;
import static io.global.ot.server.GlobalOTNodeImpl.DEFAULT_RETRY_POLICY;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<GlobalOTNodeImpl> ofGlobalOTNodeImpl(Config config) {
		return node -> node
				.withPollMasterRepositories(config.get(ofBoolean(), "pollMasters", DEFAULT_POLL_MASTER_REPOSITORIES))
				.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy", DEFAULT_RETRY_POLICY));
	}
}
