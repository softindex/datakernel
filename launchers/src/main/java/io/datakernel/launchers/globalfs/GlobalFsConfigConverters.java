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
import io.datakernel.util.ParserFunction;
import io.global.common.*;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.ot.api.RepoID;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofMemSizeAsLong;
import static io.global.fs.api.CheckpointPosStrategy.*;

public final class GlobalFsConfigConverters {
	private GlobalFsConfigConverters() {
		throw new AssertionError("nope.");
	}

	public static ConfigConverter<RawServerId> ofRawServerId() {
		return ofInetSocketAddress().transform(RawServerId::new, RawServerId::getInetSocketAddress);
	}

	private static <T extends StringIdentity> ConfigConverter<T> ofStringIdentity(ParserFunction<String, T> from) {
		return new SimpleConfigConverter<T>() {
			@Override
			protected T fromString(String string) {
				try {
					return from.parse(string);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			protected String toString(T value) {
				return value.asString();
			}
		};

	}

	public static ConfigConverter<RepoID> ofRepoID() {
		return ofStringIdentity(RepoID::fromString);
	}

	public static ConfigConverter<PubKey> ofPubKey() {
		return ofStringIdentity(PubKey::fromString);
	}

	public static ConfigConverter<PrivKey> ofPrivKey() {
		return ofStringIdentity(PrivKey::fromString);
	}

	public static ConfigConverter<SimKey> ofSimKey() {
		return ofStringIdentity(SimKey::fromString);
	}

	public static ConfigConverter<Hash> ofHash() {
		return ofStringIdentity(Hash::fromString);
	}

	public static ConfigConverter<CheckpointPosStrategy> ofCheckpointPositionStrategy() {
		return new ConfigConverter<CheckpointPosStrategy>() {

			@Override
			public CheckpointPosStrategy get(Config config) {
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
			public CheckpointPosStrategy get(Config config, @Nullable CheckpointPosStrategy defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}
}
