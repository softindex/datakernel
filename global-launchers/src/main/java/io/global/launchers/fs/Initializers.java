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

package io.global.launchers.fs;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.global.fs.local.GlobalFsNodeImpl;

import static io.datakernel.config.ConfigConverters.ofBoolean;
import static io.datakernel.config.ConfigConverters.ofInteger;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<GlobalFsNodeImpl> ofGlobalFsNodeImpl(Config config) {
		return node -> node
				.withDownloadCaching(config.get(ofBoolean(), "enableDownloadCaching", false))
				.withUploadCaching(config.get(ofBoolean(), "enableUploadCaching", false))
				.withUploadRedundancy(config.get(ofInteger(), "uploadSuccessNumber", 0),
						config.get(ofInteger(), "uploadCallNumber", 1));
	}
}
