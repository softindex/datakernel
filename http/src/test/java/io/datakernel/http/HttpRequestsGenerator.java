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

package io.datakernel.http;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Stopwatch;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.util.Preconditions.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class HttpRequestsGenerator {
	private static final int defaultProcessTimeSeconds = 10;
	private static final int defaultRequestsPerSecond = 100;
	private static final int defaultTimeoutResponse = 1000;

	static final class GeneratorOptions {
		private final int processTimeSeconds;
		private final int requestsPerSecond;
		private final int timeoutResponse;
		private final boolean displayResponse;

		private String url;

		public GeneratorOptions(String url, int processTimeSeconds, int requestsPerSecond, int timeoutResponse, boolean displayResponse) {
			checkArgument(processTimeSeconds >= 0, "processTimeSeconds must be positive value, got %s", processTimeSeconds);
			checkArgument(requestsPerSecond > 0, "requestsPerSecond must be positive value, got %s", requestsPerSecond);
			checkArgument(timeoutResponse > 10, "timeoutResponse must be larger than 10, got %s", timeoutResponse);
			this.url = url;
			this.processTimeSeconds = processTimeSeconds;
			this.requestsPerSecond = requestsPerSecond;
			this.timeoutResponse = timeoutResponse;
			this.displayResponse = displayResponse;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		int requestsPerSecond() {
			return requestsPerSecond;
		}

		int processTimeSeconds() {
			return processTimeSeconds;
		}

		int timeoutResponse() {
			return timeoutResponse;
		}

		String getUrl() {
			return url;
		}

		boolean displayResponse() {
			return displayResponse;
		}

		boolean permanentProcess() {
			return processTimeSeconds == 0;
		}

		public static GeneratorOptions parseCommandLine(String[] args) {
			int processTimeSeconds = defaultProcessTimeSeconds;
			int requestsPerSecond = defaultRequestsPerSecond;
			int timeoutResponse = defaultTimeoutResponse;
			String url = null;
			boolean displayResponse = false;

			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
					case "-n":
						requestsPerSecond = Integer.parseInt(args[++i]);
						break;
					case "-s":
						processTimeSeconds = Integer.parseInt(args[++i]);
						break;
					case "-t":
						timeoutResponse = Integer.parseInt(args[++i]);
						break;
					case "-u":
						url = args[++i];
						break;
					case "-d":
						displayResponse = true;
						break;
					case "-h":
					case "-?":
						usage();
						return null;
				}
			}
			return new GeneratorOptions(url, processTimeSeconds, requestsPerSecond, timeoutResponse, displayResponse);
		}

		public static void usage() {
			System.err.println(HttpRequestsGenerator.class.getSimpleName() + " [options]\n" +
					"\t-n    - amount requests per second (default:" + defaultRequestsPerSecond + ")\n" +
					"\t-s    - time of process in seconds (0 - permanent process, default:" + defaultProcessTimeSeconds + ")\n" +
					"\t-t    - timeout for wait response in millis (default:" + defaultTimeoutResponse + " ms)\n" +
					"\t-u    - target url for send requests\n" +
					"\t-d    - display responses\n" +
					"\t-h/-? - this help.");
		}

		@Override
		public String toString() {
			String str = "Target url        : " + url;
			if (!permanentProcess()) {
				str += "\nProcess time      : " + processTimeSeconds + " sec"
						+ "\nTotal requests    : " + (processTimeSeconds * requestsPerSecond);
			}
			str += "\nTarget speed      : " + requestsPerSecond + " req/sec"
					+ "\nDisplay responses : " + (displayResponse ? "yes" : "no") + "\n";
			if (timeoutResponse < 1000)
				str += "\n! Timeout for requests is very small: " + timeoutResponse;
			return str;
		}
	}

	private final Eventloop eventloop;
	private final AsyncHttpClient client;
	private final GeneratorOptions options;
	private final CompletionCallback completionCallback;

	private long endTimestamp = 0;
	private int successfulRequests = 0;
	private int errorRequests = 0;
	private int sentRequests = 0;

	public HttpRequestsGenerator(Eventloop eventloop, GeneratorOptions options, CompletionCallback completionCallback) {
		checkNotNull(options.getUrl());
		checkArgument(!options.getUrl().isEmpty(), "Url can not be empty");

		this.eventloop = checkNotNull(eventloop);
		this.options = checkNotNull(options);
		this.client = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8")));
		this.completionCallback = checkNotNull(completionCallback);
	}

	public void start() {
		checkState(eventloop.inEventloopThread());
		final double countRequestsPerInterval = options.requestsPerSecond() < 1000 ? 1 : options.requestsPerSecond() / 1000.0;
		final int countTasks = (int) (options.requestsPerSecond() / countRequestsPerInterval);
		final int interval = (countTasks > 1000) ? 1 : 1000 / countTasks;

		final ResultCallback<HttpResponse> callback = new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				if (options.displayResponse()) {
					ByteBuf body = result.getBody();
					if (body == null) {
						System.out.println("Response empty");
					} else {
						try {
							System.out.println(ByteBufStrings.decodeUtf8(body));
						} catch (ParseException e) {
							onException(e);
						}
					}
				}
				successfulRequests++;
			}

			@Override
			public void onException(Exception exception) {
				errorRequests++;
			}
		};

		endTimestamp = System.currentTimeMillis() + (options.processTimeSeconds() * 1000) + options.timeoutResponse();
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				scheduleTask(callback, countRequestsPerInterval, interval);
			}
		});
	}

	private void scheduleTask(final ResultCallback<HttpResponse> callback, final double requestsPerInterval, final int intervalMillis) {
		final long scheduleTimeMillis = eventloop.currentTimeMillis();
		if (!options.permanentProcess()) {
			if (scheduleTimeMillis != 0 &&
					(scheduleTimeMillis >= endTimestamp || sentRequests >= (options.processTimeSeconds() * options.requestsPerSecond()))) {
				completionCallback.onComplete();
				return;
			}
		}

		eventloop.schedule(scheduleTimeMillis + intervalMillis, new Runnable() {
			@Override
			public void run() {
				long elapsed = (scheduleTimeMillis == 0) ? 1 : eventloop.currentTimeMillis() - scheduleTimeMillis;
				int requests = (int) ((requestsPerInterval * elapsed) / intervalMillis);
				for (int i = 0; i < requests; i++)
					client.send(HttpRequest.get(options.getUrl()), options.timeoutResponse(), callback);

				sentRequests += requests;
				if (sentRequests % options.requestsPerSecond() == 0)
					System.out.println("Sent requests: " + sentRequests);
				scheduleTask(callback, requestsPerInterval, intervalMillis);
			}
		});
	}

	public void printResults(Stopwatch stopwatch) {
		double rps = (successfulRequests + errorRequests) * 1000.0 / stopwatch.elapsed(MILLISECONDS);
		System.out.println("\nSummary Elapsed      : " + stopwatch.toString()
				+ "\nSent requests        : " + sentRequests
				+ "\nRequests per second  : " + rps
				+ "\nSuccessful responses : " + successfulRequests
				+ "\nError responses      : " + errorRequests
				+ "\nLeave responses      : " + (sentRequests - successfulRequests - errorRequests));
	}

	public static void info(GeneratorOptions options) {
		System.out.println("<" + HttpRequestsGenerator.class.getSimpleName() + ">\n" + options.toString());
	}

	public static void main(String[] args) throws Exception {
		GeneratorOptions options = GeneratorOptions.parseCommandLine(args);
		if (options == null)
			return;
		info(options);

		Eventloop eventloop = Eventloop.create();

		HttpRequestsGenerator generator = new HttpRequestsGenerator(eventloop, options, ignoreCompletionCallback());
		generator.start();

		Stopwatch stopwatch = Stopwatch.createStarted();
		eventloop.run();
		stopwatch.stop();

		generator.printResults(stopwatch);
	}
}
