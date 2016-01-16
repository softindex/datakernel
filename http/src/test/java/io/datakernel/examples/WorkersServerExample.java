/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryEventloopServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example 4.
 * Example of using PrimaryEventloopServer.
 */
public class WorkersServerExample {
	final static int PORT = 9444;
	final static int WORKERS = 4;

	/* Creates a 'worker' echo server, which will handle requests to primary server.
	The response to each request contains the number of worker server. */
	public static AsyncHttpServer echoServer(Eventloop eventloop, final int workerN) {
		return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse content = HttpResponse.create().body(
						encodeAscii("Hello world: worker #" + workerN));
				callback.onResult(content);
			}
		});
	}

	/* Creates multiple worker echo servers, each in a separate thread.
	Instantiates a PrimaryEventloopServer with the specified worker servers. */
	public static void main(String[] args) throws Exception {
		final ArrayList<AsyncHttpServer> workerServers = new ArrayList<>();

		// Create and run worker servers
		for (int i = 0; i < WORKERS; i++) {
			Eventloop eventloop = new Eventloop();
			eventloop.keepAlive(true);

			AsyncHttpServer server = echoServer(eventloop, i);
			server.listen();

			workerServers.add(server);
			new Thread(eventloop).start();
		}

		// Create PrimaryEventloopServer
		Eventloop primaryEventloop = new Eventloop();
		PrimaryEventloopServer primaryEventloopServer = PrimaryEventloopServer.create(primaryEventloop)
				.workerServers(workerServers)
				.setListenPort(PORT);
		try {
			primaryEventloopServer.listen();

			// Run PrimaryEventloopServer
			Thread primaryThread = new Thread(primaryEventloop);
			primaryThread.start();

			waitForExit();

			// Close PrimaryEventloopServer
			primaryEventloopServer.closeFuture().await();
			primaryThread.join();
		} finally {
			// Close all servers
			for (AsyncHttpServer worker : workerServers) {
				worker.closeFuture().await();
				worker.getEventloop().keepAlive(false); // end of eventloop
			}
		}
	}

	private static void waitForExit() throws IOException {
		System.out.format("Server started at http://localhost:%d/, press 'enter' to shut it down.", PORT);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {
			br.readLine();
		} catch (IOException ignore) {
		}
	}
}
