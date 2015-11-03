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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.net.ConnectSettings;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.example.CumulativeServiceHelper;
import io.datakernel.rpc.protocol.RpcException;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ConcurrentServiceCallbacks;
import io.datakernel.service.NioEventloopRunner;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CumulativeBenchmark {
	private static final int TOTAL_ROUNDS = 30;
	private static final int REQUESTS_TOTAL = 1_000_000;
	private static final int REQUESTS_AT_ONCE = 100;
	private static final int DEFAULT_TIMEOUT = 2_000;

	private static final int SERVICE_PORT = 55555;
	private static final ImmutableList<InetSocketAddress> addresses = ImmutableList.of(new InetSocketAddress(SERVICE_PORT));

	private final NioEventloop serverEventloop = new NioEventloop();
	private final RpcServer server = CumulativeServiceHelper.createServer(serverEventloop, SERVICE_PORT);
	private final NioEventloopRunner serverRunner = new NioEventloopRunner(serverEventloop).addNioServers(server);

	private final NioEventloop eventloop = new NioEventloop();
	private final RpcClient client = CumulativeServiceHelper.createClient(eventloop, addresses, new ConnectSettings(500));

	private final CumulativeServiceHelper.ValueMessage incrementMessage;
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
		this.incrementMessage = new CumulativeServiceHelper.ValueMessage(2);
	}

	private void printBenchmarkInfo() {
		System.out.println("Benchmark rounds   : " + totalRounds);
		System.out.println("Requests per round : " + roundRequests);
		System.out.println("Requests at once   : " + requestsAtOnce);
		System.out.println("Increment value    : " + incrementMessage.value);
		System.out.println("Request timeout    : " + requestTimeout + " ms");
		System.out.println("RpcMessage size    : " + CumulativeServiceHelper.calculateRpcMessageSize(incrementMessage) + " bytes");
	}

	private void run() throws Exception {
		printBenchmarkInfo();

		ConcurrentServiceCallbacks.CountDownServiceCallback startServiceCallback = ConcurrentServiceCallbacks.withCountDownLatch();
		serverRunner.startFuture(startServiceCallback);
		startServiceCallback.await();

		try {
			final CompletionCallback finishCallback = new CompletionCallback() {
				@Override
				public void onException(Exception exception) {
					System.err.println("Exception while benchmark: " + exception);
					client.stop();
				}

				@Override
				public void onComplete() {
					client.stop();
				}
			};

			CompletionCallback startCallback = new CompletionCallback() {
				@Override
				public void onComplete() {
					startBenchmarkRound(0, finishCallback);
				}

				@Override
				public void onException(Exception exception) {
					finishCallback.onException(exception);
				}
			};

			client.start(startCallback);

			eventloop.run();

		} finally {
			ConcurrentServiceCallbacks.CountDownServiceCallback callbackStop = ConcurrentServiceCallbacks.withCountDownLatch();
			serverRunner.stopFuture(callbackStop);
			callbackStop.await();
		}
	}

	private int success;
	private int errors;
	private int overloads;
	private int lastResponseValue;

	private void startBenchmarkRound(final int roundNumber, final CompletionCallback finishCallback) {
		if (roundNumber == totalRounds) {
			finishCallback.onComplete();
			return;
		}

		success = 0;
		errors = 0;
		overloads = 0;
		lastResponseValue = 0;

		final Stopwatch stopwatch = Stopwatch.createUnstarted();
		final CompletionCallback roundComplete = new CompletionCallback() {
			@Override
			public void onComplete() {
				stopwatch.stop();
				System.out.println((roundNumber + 1) + ": Summary Elapsed " + stopwatch.toString()
						+ " rps: " + roundRequests * 1000.0 / stopwatch.elapsed(MILLISECONDS)
						+ " (" + success + "/" + roundRequests + " with " + overloads + " overloads) sum=" + lastResponseValue);

				eventloop.post(new Runnable() {
					@Override
					public void run() {
						startBenchmarkRound(roundNumber + 1, finishCallback);
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				finishCallback.onException(exception);
			}
		};

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				stopwatch.start();
				sendRequests(roundRequests, roundComplete);
			}
		});
	}

	private boolean clientOverloaded;

	private void sendRequests(final int numberRequests, final CompletionCallback completionCallback) {
		clientOverloaded = false;

		for (int i = 0; i < requestsAtOnce; i++) {
			if (i >= numberRequests)
				return;

			client.sendRequest(incrementMessage, requestTimeout, new ResultCallback<CumulativeServiceHelper.ValueMessage>() {
				@Override
				public void onResult(CumulativeServiceHelper.ValueMessage result) {
					success++;
					lastResponseValue = result.value;
					tryCompete();
				}

				private void tryCompete() {
					int totalCompletion = success + errors;
					if (totalCompletion == roundRequests)
						completionCallback.onComplete();
				}

				@Override
				public void onException(Exception exception) {
					if (exception.getClass() == RpcException.class) {
						clientOverloaded = true;
						overloads++;
					} else {
						errors++;
					}
					tryCompete();
				}
			});

			if (clientOverloaded) {
				// post to next event loop
				scheduleContinue(numberRequests - i, completionCallback);
				return;
			}
		}
		postContinue(numberRequests - requestsAtOnce, completionCallback);
	}

	private void postContinue(final int numberRequests, final CompletionCallback completionCallback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				sendRequests(numberRequests, completionCallback);
			}
		});
	}

	private void scheduleContinue(final int numberRequests, final CompletionCallback completionCallback) {
		eventloop.schedule(eventloop.currentTimeMillis() + 1, new Runnable() {
			@Override
			public void run() {
				sendRequests(numberRequests, completionCallback);
			}
		});
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
