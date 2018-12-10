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

import io.datakernel.annotation.Nullable;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.global.fs.api.CheckpointPosStrategy;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.ConfigConverters.ofMemSizeAsLong;
import static io.global.fs.api.CheckpointPosStrategy.of;

public final class GlobalFsConfigConverters {
	private GlobalFsConfigConverters() {
		throw new AssertionError("nope.");
	}

	public static ConfigConverter<CheckpointPosStrategy> ofCheckpointPositionStrategy() {
		return new ConfigConverter<CheckpointPosStrategy>() {

			@Override
			public CheckpointPosStrategy get(Config config) {
				return of(config.get(ofMemSizeAsLong(), THIS));
			}

			@Nullable
			@Override
			public CheckpointPosStrategy get(Config config, @Nullable CheckpointPosStrategy defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}
}
