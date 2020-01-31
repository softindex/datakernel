package io.global.launchers.pm;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.global.pm.GlobalPmNodeImpl;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofRetryPolicy;
import static io.global.kv.GlobalKvNodeImpl.DEFAULT_RETRY_POLICY;
import static io.global.kv.GlobalKvNodeImpl.DEFAULT_SYNC_MARGIN;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<GlobalPmNodeImpl> ofGlobalPmNodeImpl(Config config) {
		return node -> node
				.withSyncMargin(config.get(ofDuration(), "syncMargin", DEFAULT_SYNC_MARGIN))
				.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy", DEFAULT_RETRY_POLICY));
	}
}
