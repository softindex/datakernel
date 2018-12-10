package io.global.launchers.ot;

import io.datakernel.config.ConfigConverter;
import io.global.ot.api.RepoID;

import static io.datakernel.config.ConfigConverters.ofString;

public class GlobalOTConfigConverters {
	public static ConfigConverter<RepoID> ofRepoID() {
		return ofString().transform(RepoID::fromString, RepoID::asString);
	}
}
