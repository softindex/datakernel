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


package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.worker.Primary;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;

import static io.datakernel.config.ConfigConverters.ofInteger;

// [START EXAMPLE]
public class HttpHelloWorldModule extends AbstractModule {
	@Provides
	@Singleton
	WorkerPool workerPool(Config config) {
		return new WorkerPool(config.get(ofInteger(), "workers", 4));
	}

	@Provides
	@Singleton
	@Primary
	Eventloop primaryEventloop() {
		return Eventloop.create();
	}

	@Provides
	@Singleton
	PrimaryServer primaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
		int port = config.get(ofInteger(), "port", 5577);
		return PrimaryServer.create(primaryEventloop, workerPool.getInstances(AsyncHttpServer.class)).withListenPort(port);
	}

	@Provides
	@Worker
	Eventloop workerEventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	AsyncHttpServer workerHttpServer(Eventloop eventloop, @WorkerId final int workerId, Config config) {
		String responseMessage = config.get("message", "Some msg");
		SimpleServlet servlet = new SimpleServlet(workerId, responseMessage);
		return AsyncHttpServer.create(eventloop, servlet);
	}
}
// [END EXAMPLE]
