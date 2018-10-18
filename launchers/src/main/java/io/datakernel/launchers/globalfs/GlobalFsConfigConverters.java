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

package io.datakernel.launchers.globalfs;

import io.datakernel.annotation.Nullable;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.datakernel.config.SimpleConfigConverter;
import io.datakernel.exception.ParseException;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.fs.api.CheckpointPositionStrategy;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofMemSizeAsLong;
import static io.global.fs.api.CheckpointPositionStrategy.*;

public final class GlobalFsConfigConverters {
	private GlobalFsConfigConverters() {
		throw new AssertionError("nope.");
	}

	public static ConfigConverter<RawServerId> ofRawServerId() {
		return ofInetSocketAddress().transform(RawServerId::new, RawServerId::getInetSocketAddress);
	}

	public static ConfigConverter<PubKey> ofPubKey() {
		return new SimpleConfigConverter<PubKey>() {
			@Nullable
			@Override
			protected PubKey fromString(@Nullable String string) {
				try {
					return PubKey.fromString(string);
				} catch (ParseException e) {
					throw new AssertionError(e);
				}
			}

			@Override
			protected String toString(PubKey value) {
				return value.asString();
			}
		};
	}

	public static ConfigConverter<PrivKey> ofPrivKey() {
		return new SimpleConfigConverter<PrivKey>() {
			@Nullable
			@Override
			protected PrivKey fromString(@Nullable String string) {
				try {
					return PrivKey.fromString(string);
				} catch (ParseException e) {
					throw new AssertionError(e);
				}
			}

			@Override
			protected String toString(PrivKey value) {
				return value.asString();
			}
		};
	}

	public static ConfigConverter<CheckpointPositionStrategy> ofCheckpointPositionStrategy() {
		return new ConfigConverter<CheckpointPositionStrategy>() {

			@Override
			public CheckpointPositionStrategy get(Config config) {
				switch (config.getValue()) {
					case "fixed":
						return fixed(config.get(ofMemSizeAsLong(), "offset"));
					case "randRange":
						return randRange(config.get(ofMemSizeAsLong(), "min"), config.get(ofMemSizeAsLong(), "max"));
					case "alterating":
						return alterating(config.get(ofMemSizeAsLong(), "first"), config.get(ofMemSizeAsLong(), "second"));
					default:
						throw new IllegalArgumentException("No checkpoint position strategy named " + config.getValue() + " exists!");
				}
			}

			@Nullable
			@Override
			public CheckpointPositionStrategy get(Config config, @Nullable CheckpointPositionStrategy defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}
}
