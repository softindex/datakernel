package io.global.launchers.kv;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.global.kv.GlobalKvNodeImpl;

import static io.datakernel.config.ConfigConverters.ofBoolean;
import static io.datakernel.config.ConfigConverters.ofInteger;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<GlobalKvNodeImpl> ofGlobalKvNodeImpl(Config config) {
		return node -> node
				.withDownloadCaching(config.get(ofBoolean(), "enableDownloadCaching", false))
				.withUploadCaching(config.get(ofBoolean(), "enableUploadCaching", false))
				.withUploadRedundancy(config.get(ofInteger(), "uploadSuccessNumber", 0),
						config.get(ofInteger(), "uploadCallNumber", 1));
	}
}
