package io.datakernel.http.boot;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.util.MemSize;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofMemSize;
import static io.datakernel.config.ConfigUtils.initializeAbstractServer;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;
import static java.lang.System.currentTimeMillis;

public class ConfigUtils {
	private ConfigUtils() {
	}

	public static void initializeHttpServer(AsyncHttpServer server, Config config) {
		initializeAbstractServer(server, config);
		server.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)));
		server.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS));
		server.withReadTimeout(config.get(ofInteger(), "readTimeout", 0));
		server.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}

	public static void initializeHttpWorker(AsyncHttpServer worker, Config config) {
		worker.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", MemSize.of(Integer.MAX_VALUE)));
		worker.withKeepAliveTimeout(config.get(ofInteger(), "keepAliveTimeout", (int) DEFAULT_KEEP_ALIVE_MILLIS));
		worker.withReadTimeout(config.get(ofInteger(), "readTimeout", 0));
		worker.withWriteTimeout(config.get(ofInteger(), "writeTimeout", 0));
	}

	public static void initializeHttpServerTriggers(AsyncHttpServer server, TriggerRegistry triggers, Config config) {
		if (server.getStats() != null) {
			int servletExceptionTtl = config.get(ConfigConverters.ofInteger(), "servletException.ttl", 5 * 60);
			double httpTimeoutsThreshold = config.get(ConfigConverters.ofDouble(), "httpTimeoutsThreshold", 1.0);
			triggers.add(HIGH, "servletExceptions", () ->
					TriggerResult.ofError(server.getStats().getServletExceptions())
							.whenTimestamp(timestamp -> timestamp > currentTimeMillis() - servletExceptionTtl * 1000L));
			triggers.add(WARNING, "httpTimeouts", () ->
					TriggerResult.ofValue(server.getStats().getHttpTimeouts().getSmoothedRate(), timeouts -> timeouts > httpTimeoutsThreshold));
		}
	}
}
