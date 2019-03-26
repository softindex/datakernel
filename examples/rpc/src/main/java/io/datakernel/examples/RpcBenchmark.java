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

import com.google.inject.*;
import com.google.inject.name.Named;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.util.Arrays.asList;


public class RpcBenchmark extends Launcher {

	private final static int MAX_REQUESTS = 1000000;
	private final static int WARMUP_ROUNDS = 1;
	private final static int BENCHMARK_ROUNDS = 3;
	private final static int SERVICE_PORT = 25565;
	private final static int REQUESTS_PER_TIME = 1000;
	private final static boolean GENERATE_FILE = false;

	@Inject
	private RpcClient rpcClient;

	@Inject
	@Named("client")
	private Eventloop eventloop;

	@Inject
	private Config config;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ConfigModule.create(Config.create()
						.with("rpc.server.port", "" + SERVICE_PORT)
						.with("benchmark.warmupRounds", "" + WARMUP_ROUNDS)
						.with("benchmark.benchmarkRounds", "" + BENCHMARK_ROUNDS)
						.with("benchmark.requestsPerTime", "" + REQUESTS_PER_TIME)
						.with("benchmark.maxRequests", "" + MAX_REQUESTS)
						.with("benchmark.generateFile", "" + GENERATE_FILE)
						.override(Config.ofProperties(System.getProperties()))),
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Provides
					@Singleton
					@Named("client")
					Eventloop eventloopClient() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Provides
					@Singleton
					@Named("server")
					Eventloop eventloopServer() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Provides
					@Singleton
					public RpcClient rpcClient(@Named("client") Eventloop eventloop, Config config) {
						return RpcClient.create(eventloop)
								.withMessageTypes(String.class)
								.withStrategy(server(new InetSocketAddress(config.get(ofInteger(), "rpc.server.port"))));
					}

					@Provides
					@Singleton
					public RpcServer rpcServer(@Named("server") Eventloop eventloop, Config config) {
						return RpcServer.create(eventloop)
								.withListenPort(config.get(ofInteger(), "rpc.server.port"))
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class, req -> Promise.of("Returning: " + req));

					}
				}
		);
	}

	private boolean generateFile;
	private int warmupRounds;
	private int benchmarkRounds;
	private long maxRequests;
	private int requestsPerTime;
	private int treshold;


	@Override
	protected void onStart() {
		generateFile = config.get(ofBoolean(), "benchmark.generateFile");
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds");
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds");
		maxRequests = config.get(ofLong(), "benchmark.maxRequests");
		requestsPerTime = config.get(ofInteger(), "benchmark.requestsPerTime");
		treshold = config.get(ofInteger(), "benchmark.requestsPerTime");
		maxRequests = config.get(ofLong(), "benchmark.maxRequests");
	}

	@Override
	protected void run() throws Exception {
		PrintWriter resultsFile = null;
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		if (generateFile) {
			resultsFile = new PrintWriter("benchmarkResult" + Timestamp.from(Instant.now()), "UTF-8");
			resultsFile.println("<table>");
			resultsFile.println("  <tr>");
			resultsFile.println("    <th>ApacheBench parameters</th>");
			resultsFile.println("    <th>Time</th>");
			resultsFile.println("    <th>Average time</th>");
			resultsFile.println("    <th>Best time</th>");
			resultsFile.println("    <th>Worst time</th>");
			resultsFile.println("    <th>Requests per second</th>");
			resultsFile.println("  </tr>");
		}

		if (warmupRounds > 0) {
			System.out.println("Warming up for " + warmupRounds + " rounds.");
		}

		for (int i = 0; i < warmupRounds; i++) {
			round();
		}

		System.out.println("Benchmarking...");
		long roundTime;

		for (int i = 0; i < benchmarkRounds; i++) {

			roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms");
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (maxRequests / avgTime * 1000);
		System.out.println("Time: " + time + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);
		if (generateFile) {
			resultsFile.println("    <td> -k -c " + requestsPerTime + "</td>");
			resultsFile.println("    <td>" + avgTime + "</td>");
			resultsFile.println("    <td>" + bestTime + "</td>");
			resultsFile.println("    <td>" + worstTime + "</td>");
			resultsFile.println("    <td>" + requestsPerSecond + "</td>");
			resultsFile.println("  </tr>");
			resultsFile.close();
			resultsFile.println("</table>");
		}
	}

	private long round() throws Exception {
		return eventloop.submit(() -> benchmark().toCompletableFuture()).get();
	}

	private static final class Counters {
		/**
		 * First counter represents amount of sent requests, so we know when to stop sending them
		 * Second counter represents amount of completed requests(in another words completed will be incremented when
		 * request fails or completes successfully) so we know when to stop round of benchmark
		 */
		int sent;
		int completed;
	}

	private Promise<Long> benchmark() {
		SettablePromise<Long> promise = new SettablePromise<>();

		long start = System.currentTimeMillis();

		Counters counters = new Counters();

		for (int i = 0; i < treshold; i++) {
			sendRequest(promise, counters, maxRequests);
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void sendRequest(SettablePromise<Long> promise, Counters counters, long maxRequests) {
		// Stop when we sent required amount of requests
		if (counters.sent == maxRequests) {
			return;
		}

		++counters.sent;

		rpcClient.sendRequest("Hello world" + counters.sent, 1000)
				.whenComplete((res, exc) -> {
					++counters.completed;

					// Stop round
					if (counters.completed == maxRequests) {
						promise.set(null);
						return;
					}

					sendRequest(promise, counters, maxRequests);
				});
	}

	public static void main(String[] args) throws Exception {
		RpcBenchmark benchmark = new RpcBenchmark();
		benchmark.launch(true, args);
	}
}
