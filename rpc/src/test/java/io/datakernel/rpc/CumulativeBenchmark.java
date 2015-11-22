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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcException;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.util.Stopwatch;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.server;
import static io.datakernel.rpc.protocol.RpcSerializer.serializerFor;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
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

	private final NioEventloop serverEventloop = new NioEventloop();
	private final NioEventloop clientEventloop = new NioEventloop();

	private final RpcServer server = RpcServer.create(serverEventloop, serializerFor(ValueMessage.class))
			.protocol(streamProtocol(64 << 10, 64 << 10, true))
			.on(ValueMessage.class, new RpcRequestHandler<ValueMessage>() {
				private final ValueMessage currentSum = new ValueMessage(0);

				@Override
				public void run(ValueMessage request, ResultCallback<Object> callback) {
					if (request.value != 0) {
						currentSum.value += request.value;
					} else {
						currentSum.value = 0;
					}
					callback.onResult(currentSum);
				}
			})
			.setListenPort(SERVICE_PORT);

	private final RpcClient client = RpcClient.create(clientEventloop, serializerFor(ValueMessage.class))
			.protocol(streamProtocol(64 << 10, 64 << 10, true))
			.strategy(server(new InetSocketAddress(SERVICE_PORT)));

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
		BufferSerializer<RpcMessage> serializer = serializerFor(ValueMessage.class).createSerializer();
		int defaultBufferSize = 100;
		SerializationOutputBuffer output;
		while (true) {
			try {
				byte[] array = new byte[defaultBufferSize];
				output = new SerializationOutputBuffer(array);
				serializer.serialize(output, new RpcMessage(12345, incrementMessage));
				break;
			} catch (ArrayIndexOutOfBoundsException e) {
				defaultBufferSize = defaultBufferSize * 2;
			}
		}
		System.out.println("RpcMessage size    : " + output.position() + " bytes");
	}

	private void run() throws Exception {
		printBenchmarkInfo();

		Executors.defaultThreadFactory().newThread(new Runnable() {
			@Override
			public void run() {
				serverEventloop.keepAlive(true);
				serverEventloop.run();
			}
		}).start();
		server.listen();

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

			clientEventloop.run();

		} finally {
			serverEventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {

					server.close();
				}
			});
			serverEventloop.keepAlive(false);

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

				clientEventloop.post(new Runnable() {
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

		clientEventloop.post(new Runnable() {
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

			client.sendRequest(incrementMessage, requestTimeout, new ResultCallback<ValueMessage>() {
				@Override
				public void onResult(ValueMessage result) {
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
		clientEventloop.post(new Runnable() {
			@Override
			public void run() {
				sendRequests(numberRequests, completionCallback);
			}
		});
	}

	private void scheduleContinue(final int numberRequests, final CompletionCallback completionCallback) {
		clientEventloop.schedule(clientEventloop.currentTimeMillis() + 1, new Runnable() {
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
