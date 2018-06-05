package io.datakernel.launchers;

import io.datakernel.config.Config;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.trigger.TriggersModule;
import io.datakernel.util.Initializer;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_INITIAL_BUFFER_SIZE;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_MAX_MESSAGE_SIZE;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;
import static java.lang.System.currentTimeMillis;

public class Initializers {
	private Initializers() {
	}

	public static <T extends AbstractServer<T>> Initializer<T> ofAbstractServer(Config config) {
		return server -> server
			.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "listenAddresses"))
			.withAcceptOnce(config.get(ofBoolean(), "acceptOnce", false))
			.withSocketSettings(config.get(ofSocketSettings(), "socketSettings", server.getSocketSettings()))
			.withServerSocketSettings(config.get(ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));

	}

	public static Initializer<PrimaryServer> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop> ofEventloop(Config config) {
		return eventloop -> eventloop
			.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "fatalErrorHandler", eventloop.getFatalErrorHandler()))
			.withIdleInterval(config.get(ofDuration(), "idleInterval", eventloop.getIdleInterval()))
			.withThreadPriority(config.get(ofInteger(), "threadPriority", eventloop.getThreadPriority()));
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
			.initialize(ofHttpWorker(config));
	}

	public static Initializer<AsyncHttpServer> ofHttpWorker(Config config) {
		return server -> server
			.withMaxHttpMessageSize(config.get(ofMemSize(), "maxMessageSize", server.getMaxHttpMessageSize()))
			.withKeepAliveTimeout(config.get(ofDuration(), "keepAliveTimeout", server.getKeepAliveTimeout()))
			.withReadTimeout(config.get(ofDuration(), "readTimeout", server.getReadTimeout()))
			.withWriteTimeout(config.get(ofDuration(), "writeTimeout", server.getWriteTimeout()));
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
				config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_INITIAL_BUFFER_SIZE),
				config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", DEFAULT_MAX_MESSAGE_SIZE),
				config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
			.withAutoFlushInterval(config.get(ofDuration(), "rpc.flushDelay", Duration.ZERO));
	}

	public static Initializer<TriggersModule> ofTriggersModule(Config config) {
		long businessLogicTimeLow = config.get(ofDurationAsMillis(), "businessLogicTimeLow", 10L);
		long businessLogicTimeHigh = config.get(ofDurationAsMillis(), "businessLogicTimeHigh", 100L);
		double throttlingLow = config.get(ofDouble(), "throttlingLow", 0.1);
		double throttlingHigh = config.get(ofDouble(), "throttlingHigh", 0.5);
		return triggersModule -> triggersModule
			.with(Eventloop.class, HIGH, "fatalErrors", eventloop ->
				TriggerResult.ofError(eventloop.getStats().getFatalErrors()))
			.with(Eventloop.class, WARNING, "businessLogic", eventloop ->
				TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
					businessLogicTime -> businessLogicTime > businessLogicTimeLow))
			.with(Eventloop.class, HIGH, "businessLogic", eventloop ->
				TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
					businessLogicTime -> businessLogicTime > businessLogicTimeHigh))
			.with(ThrottlingController.class, WARNING, "throttling", throttlingController ->
				TriggerResult.ofValue(throttlingController.getAvgThrottling(),
					throttling -> throttling > throttlingLow))
			.with(ThrottlingController.class, HIGH, "throttling", throttlingController ->
				TriggerResult.ofValue(throttlingController.getAvgThrottling(),
					throttling -> throttling > throttlingHigh))
			;
	}

}
