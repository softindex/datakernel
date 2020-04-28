/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.launchers.initializers;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.common.Initializer;
import io.datakernel.common.MemSize;
import io.datakernel.config.Config;
import io.datakernel.di.core.Key;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModuleSettings;
import io.datakernel.net.AbstractServer;
import io.datakernel.net.PrimaryServer;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.*;

public class Initializers {
	public static final String GLOBAL_EVENTLOOP_NAME = "GlobalEventloopStats";
	public static final Key<Eventloop> GLOBAL_EVENTLOOP_KEY = Key.ofName(Eventloop.class, GLOBAL_EVENTLOOP_NAME);

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

	public static Initializer<EventloopTaskScheduler> ofEventloopTaskScheduler(Config config) {
		return scheduler -> {
			scheduler.setEnabled(!config.get(ofBoolean(), "disabled", false));
			scheduler.withAbortOnError(config.get(ofBoolean(), "abortOnError", false))
					.withInitialDelay(config.get(ofDuration(), "initialDelay", Duration.ZERO))
					.withSchedule(config.get(ofEventloopTaskSchedule(), "schedule", null))
					.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy"));
		};
	}

	public static Initializer<AsyncHttpServer> ofHttpServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config))
				.initialize(ofHttpWorker(config));
	}

	public static Initializer<AsyncHttpServer> ofHttpWorker(Config config) {
		return server -> server
				.withKeepAliveTimeout(config.get(ofDuration(), "keepAliveTimeout", server.getKeepAliveTimeout()))
				.withReadWriteTimeout(config.get(ofDuration(), "readWriteTimeout", server.getReadWriteTimeout()))
				.withMaxBodySize(config.get(ofMemSize(), "maxBodySize", MemSize.ZERO));
	}

	public static Initializer<JmxModuleSettings> ofGlobalEventloopStats() {
		return jmxModule -> jmxModule
				.withGlobalMBean(Eventloop.class, GLOBAL_EVENTLOOP_KEY)
				.withOptional(GLOBAL_EVENTLOOP_KEY, "fatalErrors_total")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "businessLogicTime_smoothedAverage")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "loops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "loops_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_smoothedRate");
	}
}
