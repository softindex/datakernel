package io.datakernel.http.boot;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.util.Initializer;
import io.datakernel.util.MemSize;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofMemSize;
import static io.datakernel.config.ConfigInitializers.ofAbstractServer;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;
import static java.lang.System.currentTimeMillis;

public class ConfigInitializers {
	private ConfigInitializers() {
	}

	public static Initializer<AsyncHttpServer> ofHttpServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config))
				.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)))
				.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS))
				.withReadTimeout(config.get(ofInteger(), "readTimeout", 0))
				.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}

	public static Initializer<AsyncHttpServer> ofHttpWorker(Config config) {
		return worker -> worker
				.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)))
				.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS))
				.withReadTimeout(config.get(ofInteger(), "readTimeout", 0))
				.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}

	public static Initializer<AsyncHttpServer> ofHttpServerTriggers(TriggerRegistry triggers, Config config) {
		return server -> {
			if (server.getStats() == null) return;
			int servletExceptionTtl = config.get(ofInteger(), "servletException.ttl", 5 * 60);
			double httpTimeoutsThreshold = config.get(ConfigConverters.ofDouble(), "httpTimeoutsThreshold", 1.0);
			triggers.add(HIGH, "servletExceptions", () ->
					TriggerResult.ofError(server.getStats().getServletExceptions())
							.whenTimestamp(timestamp -> timestamp > currentTimeMillis() - servletExceptionTtl * 1000L));
			triggers.add(WARNING, "httpTimeouts", () ->
					TriggerResult.ofValue(server.getStats().getHttpTimeouts().getSmoothedRate(), timeouts -> timeouts > httpTimeoutsThreshold));
		};
	}
}
