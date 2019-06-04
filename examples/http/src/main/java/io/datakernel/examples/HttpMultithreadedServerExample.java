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
	@Provides
	@Worker
	AsyncServlet servlet(@WorkerId int workerId) {
		return request -> {
			byte[] msg = encodeAscii("Hello from worker server #" + workerId + "\n");
			return Promise.of(HttpResponse.ok200().withBody(msg));
		};
	}

	public static void main(String[] args) throws Exception {
		HttpMultithreadedServerExample example = new HttpMultithreadedServerExample();
		example.launch(args);
	}
}
