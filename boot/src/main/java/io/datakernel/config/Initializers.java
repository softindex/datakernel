package io.datakernel.config;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.util.Initializer;
import io.datakernel.util.MemSize;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_KEEP_ALIVE_MILLIS;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_PACKET_SIZE;
import static io.datakernel.rpc.server.RpcServer.MAX_PACKET_SIZE;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;
import static java.lang.System.currentTimeMillis;

public class Initializers {
	private Initializers() {
	}

	public static <T extends AbstractServer<T>> Initializer<T> ofAbstractServer(Config config) {
		return server -> server
				.withListenAddresses(config.get(ConfigConverters.ofList(ConfigConverters.ofInetSocketAddress()), "listenAddresses"))
				.withAcceptOnce(config.get(ConfigConverters.ofBoolean(), "acceptOnce", false))
				.withSocketSettings(config.get(ConfigConverters.ofSocketSettings(), "socketSettings", server.getSocketSettings()))
				.withServerSocketSettings(config.get(ConfigConverters.ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));

	}

	public static Initializer<PrimaryServer> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop> ofEventloop(Config config) {
		return eventloop -> eventloop
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "fatalErrorHandler", rethrowOnAnyError()))
				.withIdleInterval(config.get(ofLong(), "idleIntervalMillis", DEFAULT_IDLE_INTERVAL))
				.withThreadPriority(config.get(ofInteger(), "threadPriority", 0));
	}

	public static Initializer<Eventloop> ofEventloopTriggers(TriggerRegistry triggersRegistry, Config config) {
		return eventloop -> {
			int businessLogicTimeWarning = config.get(ofInteger(), "businessLogicTime.warning", 10);
			int businessLogicTimeHigh = config.get(ofInteger(), "businessLogicTime.high", 100);
			triggersRegistry.add(HIGH, "fatalErrors", () ->
					TriggerResult.ofError(eventloop.getStats().getFatalErrors()));
			triggersRegistry.add(WARNING, "businessLogic", () ->
					TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
							businessLogicTime -> businessLogicTime > businessLogicTimeWarning));
			triggersRegistry.add(HIGH, "businessLogic", () ->
					TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
							businessLogicTime -> businessLogicTime > businessLogicTimeHigh));
		};
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
			double httpTimeoutsThreshold = config.get(ofDouble(), "httpTimeoutsThreshold", 1.0);
			triggers.add(HIGH, "servletExceptions", () ->
					TriggerResult.ofError(server.getStats().getServletExceptions())
							.whenTimestamp(timestamp -> timestamp > currentTimeMillis() - servletExceptionTtl * 1000L));
			triggers.add(WARNING, "httpTimeouts", () ->
					TriggerResult.ofValue(server.getStats().getHttpTimeouts().getSmoothedRate(), timeouts -> timeouts > httpTimeoutsThreshold));
		};
	}

	public static Initializer<RemoteFsServer> ofRemoteFsServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config));
	}

	public static Initializer<RemoteFsServer> ofRemoteFsServerTriggers(TriggerRegistry triggerRegistry, Config config) {
		return server -> {}; // TODO
	}

	public static Initializer<RpcServer> ofRpcServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config.getChild("rpc.server")))
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_PACKET_SIZE),
						config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", MAX_PACKET_SIZE),
						config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
				.withFlushDelay(config.get(ofInteger(), "rpc.flushDelay", 0));
	}
}
