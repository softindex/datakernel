package io.global.launchers;

import io.datakernel.config.ConfigConverter;
import io.global.common.*;

import static io.datakernel.config.ConfigConverters.ofString;

public class GlobalConfigConverters {
	public static ConfigConverter<RawServerId> ofRawServerId() {
		return ofString().transform(RawServerId::new, RawServerId::getServerIdString);
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
		return ofString().transform(Hash::parseString, Hash::asString);
	}
}
