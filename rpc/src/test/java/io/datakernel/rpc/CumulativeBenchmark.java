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

package io.datakernel.rpc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcException;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.util.Stopwatch;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.util.MemSize.kilobytes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CumulativeBenchmark {
	public static final class ValueMessage {
		@Serialize(order = 0)
		public int value;

		public ValueMessage(@Deserialize("value") int value) {
			this.value = value;
		}
	}

	private static final int TOTAL_ROUNDS = 30;
	private static final int REQUESTS_TOTAL = 1_000_000;
	private static final int REQUESTS_AT_ONCE = 100;
	private static final int DEFAULT_TIMEOUT = 2_000;

	private static final int SERVICE_PORT = 55555;

	private final Eventloop serverEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private final Eventloop clientEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

	private final RpcServer server = RpcServer.create(serverEventloop)
			.withStreamProtocol(kilobytes(64), kilobytes(64), true)
			.withMessageTypes(ValueMessage.class)
			.withHandler(ValueMessage.class, ValueMessage.class, new RpcRequestHandler<ValueMessage, ValueMessage>() {
				private final ValueMessage currentSum = new ValueMessage(0);

				@Override
				public CompletionStage<ValueMessage> run(ValueMessage request) {
					if (request.value != 0) {
						currentSum.value += request.value;
					} else {
						currentSum.value = 0;
					}
					return SettableStage.immediateStage(currentSum);
				}
			})
			.withListenAddress(new InetSocketAddress("localhost", SERVICE_PORT));

	private final RpcClient client = RpcClient.create(clientEventloop)
			.withMessageTypes(ValueMessage.class)
			.withStreamProtocol(kilobytes(64), kilobytes(64), true)
			.withStrategy(server(new InetSocketAddress("localhost", SERVICE_PORT)));

	private final ValueMessage incrementMessage;
	private final int totalRounds;
	private final int roundRequests;
	private final int requestsAtOnce;
	private final int requestTimeout;

	public CumulativeBenchmark() {
		this(TOTAL_ROUNDS, REQUESTS_TOTAL, REQUESTS_AT_ONCE, DEFAULT_TIMEOUT);
	}

	public CumulativeBenchmark(int totalRounds, int roundRequests, int requestsAtOnce, int requestTimeout) {
		this.totalRounds = totalRounds;
		this.roundRequests = roundRequests;
		this.requestsAtOnce = requestsAtOnce;
		this.requestTimeout = requestTimeout;
		this.incrementMessage = new ValueMessage(2);
	}

	private void printBenchmarkInfo() {
		System.out.println("Benchmark rounds   : " + totalRounds);
		System.out.println("Requests per round : " + roundRequests);
		System.out.println("Requests at once   : " + requestsAtOnce);
		System.out.println("Increment value    : " + incrementMessage.value);
		System.out.println("Request timeout    : " + requestTimeout + " ms");
	}

	private void run() throws Exception {
		printBenchmarkInfo();

		server.listen();
		Executors.defaultThreadFactory().newThread(serverEventloop).start();

		try {
			client.start().whenComplete((aVoid, throwable) -> {
				if (throwable == null) {
					System.out.println("----------------------------------------");
					System.out.printf("Average time elapsed per round: %.1f ms\n", totalElapsed / (double) totalRounds);
					try {
						client.stopFuture();
						server.closeFuture();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					System.err.println("Exception while benchmark: " + throwable);
					try {
						client.stopFuture();
						server.closeFuture();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});

			clientEventloop.run();

		} finally {
			serverEventloop.execute(server::close);
			serverEventloop.keepAlive(false);

		}
	}

	private int success;
	private int errors;
	private int overloads;
	private int lastResponseValue;

	private int totalElapsed = 0;

	private CompletionStage<Void> startBenchmarkRound(final int roundNumber) {
		final SettableStage<Void> stage = SettableStage.create();
		startBenchmarkRound(roundNumber, stage);
		return stage;
	}

	private void startBenchmarkRound(final int roundNumber, final SettableStage<Void> stage) {
		if (roundNumber == totalRounds) {
			stage.set(null);
			return;
		}

		success = 0;
		errors = 0;
		overloads = 0;
		lastResponseValue = 0;

		final Stopwatch stopwatch = Stopwatch.createUnstarted();
		clientEventloop.post(() -> {
			stopwatch.start();
			sendRequests(roundRequests).thenAccept(aVoid -> {
				stopwatch.stop();
				totalElapsed += stopwatch.elapsed(MILLISECONDS);

				System.out.println((roundNumber + 1) + ": Summary Elapsed " + stopwatch.toString()
						+ " rps: " + roundRequests * 1000.0 / stopwatch.elapsed(MILLISECONDS)
						+ " (" + success + "/" + roundRequests + " with " + overloads + " overloads) sum=" + lastResponseValue);

				clientEventloop.post(() -> startBenchmarkRound(roundNumber + 1, stage));
			});
		});
	}

	private boolean clientOverloaded;

	private CompletionStage<Void> sendRequests(final int numberRequests) {
		final SettableStage<Void> stage = SettableStage.create();
		sendRequests(numberRequests, stage);
		return stage;
	}

	private void sendRequests(final int numberRequests, final SettableStage<Void> stage) {
		clientOverloaded = false;

		for (int i = 0; i < requestsAtOnce; i++) {
			if (i >= numberRequests)
				return;

			client.<ValueMessage, ValueMessage>sendRequest(incrementMessage, requestTimeout).whenComplete(new BiConsumer<ValueMessage, Throwable>() {
				@Override
				public void accept(ValueMessage valueMessage, Throwable throwable) {
					if (throwable != null) {
						if (throwable.getClass() == RpcException.class) {
							clientOverloaded = true;
							overloads++;
						} else {
							errors++;
						}
						tryCompete();
					} else {
						success++;
						lastResponseValue = valueMessage.value;
						tryCompete();
					}
				}

				private void tryCompete() {
					int totalCompletion = success + errors;
					if (totalCompletion == roundRequests)
						stage.set(null);
				}
			});

			if (clientOverloaded) {
				// post to next event loop
				scheduleContinue(numberRequests - i, stage);
				return;
			}
		}
		postContinue(numberRequests - requestsAtOnce, stage);
	}

	private void postContinue(final int numberRequests, SettableStage<Void> stage) {
		clientEventloop.post(() -> sendRequests(numberRequests).whenComplete(AsyncCallbacks.forwardTo(stage)));
	}

	private void scheduleContinue(final int numberRequests, SettableStage<Void> stage) {
		clientEventloop.schedule(clientEventloop.currentTimeMillis() + 1, () ->
				sendRequests(numberRequests).whenComplete(AsyncCallbacks.forwardTo(stage)));
	}

	public static void main(String[] args) throws Exception {
		loggerLevel(Level.OFF);

		new CumulativeBenchmark().run();
	}

	private static void loggerLevel(Level level) {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(level);
	}

}
