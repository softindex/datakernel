package io.global.launchers.kv;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.global.kv.GlobalKvNodeImpl;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofRetryPolicy;
import static io.global.kv.GlobalKvNodeImpl.DEFAULT_RETRY_POLICY;
import static io.global.kv.GlobalKvNodeImpl.DEFAULT_SYNC_MARGIN;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static Initializer<GlobalKvNodeImpl> ofGlobalKvNodeImpl(Config config) {
		return node -> node
				.withSyncMargin(config.get(ofDuration(), "syncMargin", DEFAULT_SYNC_MARGIN))
				.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy", DEFAULT_RETRY_POLICY));
	}
}
