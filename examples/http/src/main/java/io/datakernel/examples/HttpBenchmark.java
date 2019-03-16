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
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.util.Arrays.asList;

@SuppressWarnings("unused")
public class HttpBenchmark extends Launcher {

	private final static int MAX_REQUESTS = 1000000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 12;
	private final static int REQUESTS_PER_TIME = 100;

	@Inject
	Config config;

	@Inject
	@Named("client")
	Eventloop eventloop;

	@Inject
	AsyncHttpClient httpClient;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.create()
						.with("benchmark.warmupRounds", "" + WARMUP_ROUNDS)
						.with("benchmark.benchmarkRounds", "" + BENCHMARK_ROUNDS)
						.with("benchmark.maxRequests", "" + MAX_REQUESTS)
						.with("benchmark.requestsPerTime", "" + REQUESTS_PER_TIME)
						.with("http.googlePublicDns", "8.8.8.8")
						.with("server.http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(25565)))
						.with("server.http.socketSettings.keepAlive", "false")
						.with("server.http.socketSettings.tcpNoDelay", "true")
						.with("client.http.port", "25565")
						.with("client.http.socketSettings.keepAlive", "false")
						.with("client.http.socketSettings.tcpNoDelay", "true")
				),
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
					AsyncDnsClient dnsClient(@Named("client") Eventloop eventloop, Config config) {
						return RemoteAsyncDnsClient.create(eventloop)
								.withDnsServerAddress(config.get(ofInetAddress(), "http.googlePublicDns"));
					}

					@Provides
					@Singleton
					AsyncHttpClient httpClient(@Named("client") Eventloop eventloop, AsyncDnsClient dnsClient, Config config) {
						return AsyncHttpClient.create(eventloop).withDnsClient(dnsClient)
								.withSocketSettings(config.get(ofSocketSettings(), "client.http.socketSettings"));
					}

					@Provides
					@Singleton
					AsyncHttpServer httpServer(@Named("server") Eventloop eventloop, AsyncServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("server.http")));
					}

					@Provides
					@Singleton
					AsyncServlet servlet() {
						return ignored -> Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello world!")));
					}
				}
		);
	}

	private String addr;
	private int warmupRounds;
	private int benchmarkRounds;
	private long maxRequests;
	private int treshold;

	private void printSocketSettings(Config config) {
		System.out.println("SocketSettings:\n\tclient:");
		System.out.println("\t\tTCP Keep Alive: " + config.get("client.http.socketSettings.keepAlive"));
		System.out.println("\t\tTCP No Delay: " + config.get("client.http.socketSettings.tcpNoDelay"));
		System.out.println("\tserver:");
		System.out.println("\t\tTCP Keep Alive: " + config.get("server.http.socketSettings.keepAlive"));
		System.out.println("\t\tTCP No Delay: " + config.get("server.http.socketSettings.tcpNoDelay"));
	}

	@Override
	protected void onStart() {
		printSocketSettings(config);
		addr = "http://127.0.0.1:" + config.get(ofInteger(), "client.http.port");
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds");
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds");
		maxRequests = config.get(ofLong(), "benchmark.maxRequests");
		treshold = config.get(ofInteger(), "benchmark.requestsPerTime");
		maxRequests = config.get(ofLong(), "benchmark.maxRequests");
	}

	@Override
	protected void run() throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		if (warmupRounds > 0) {
			System.out.println("Warming up for " + warmupRounds + " rounds.");
		}

		for (int i = 0; i < warmupRounds; i++) {
			round();
		}

		long roundTime;

		System.out.println("Benchmarking...");
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
		double avgTime = time / benchmarkRounds;
		long requestsPerSecond = (long) (maxRequests / avgTime * 1000);
		System.out.println("Time: " + time + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);
	}

	private long round() throws ExecutionException, InterruptedException {
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
		HttpRequest request = HttpRequest.get(addr);

		httpClient.request(request)
				.acceptEx((res, e) -> {
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
		HttpBenchmark benchmark = new HttpBenchmark();
		benchmark.launch(true, args);
	}
}
