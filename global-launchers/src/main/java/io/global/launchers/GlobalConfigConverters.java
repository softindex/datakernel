package io.global.launchers;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.global.common.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;

import static io.datakernel.config.ConfigConverters.ofString;

public class GlobalConfigConverters {
	public static ConfigConverter<RawServerId> ofRawServerId() {
		return new ConfigConverter<RawServerId>() {
			@Nullable
			@Override
			public RawServerId get(Config config, @Nullable RawServerId defaultValue) {
				try {
					return get(config);
				} catch (NoSuchElementException ignored) {
					return defaultValue;
				}
			}

			@NotNull
			@Override
			public RawServerId get(Config config) {
				return new RawServerId(config.getValue());
			}
		};
	}

	public static ConfigConverter<PubKey> ofPubKey() {
		return ofString().transform(PubKey::fromString, PubKey::asString);
	}

	public static ConfigConverter<PrivKey> ofPrivKey() {
		return ofString().transform(PrivKey::fromString, PrivKey::asString);
	}

	public static ConfigConverter<SimKey> ofSimKey() {
		return ofString().transform(SimKey::fromString, SimKey::asString);
	}

	public static ConfigConverter<Hash> ofHash() {
		return ofString().transform(Hash::fromString, Hash::asString);
	}
}
