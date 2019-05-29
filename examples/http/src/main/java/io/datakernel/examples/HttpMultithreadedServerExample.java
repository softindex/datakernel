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

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launchers.http.MultithreadedHttpServerLauncher;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

/**
 * HTTP multithreaded server example.
 * Sends back a greeting and the number of worker which served the connection.
 */
public final class HttpMultithreadedServerExample extends MultithreadedHttpServerLauncher {
	public static final int PORT = 7583;
	public static final int WORKERS = 4;

	@Override
	protected Module getOverrideModule() {
		return ConfigModule.create(Config.create()
				.with("http.listenAddresses", "" + PORT)
				.with("workers", "" + WORKERS));
	}

	@Override
	protected Module getBusinessLogicModule() {
		return new AbstractModule() {
			@Provides
			@Worker
			AsyncServlet servlet(@WorkerId final int workerId) {
				return request -> {
					byte[] msg = encodeAscii("Hello from worker server #" + workerId + "\n");
					return Promise.of(HttpResponse.ok200().withBody(msg));
				};
			}
		};
	}

	@Override
	protected void run() throws Exception {
		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:7583/'");
		super.run();
	}

	public static void main(String[] args) throws Exception {
		HttpMultithreadedServerExample example = new HttpMultithreadedServerExample();
		example.launch(args);
	}
}
