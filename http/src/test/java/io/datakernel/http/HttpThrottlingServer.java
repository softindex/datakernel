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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.jmx.MBeanFormat;
import io.datakernel.util.ByteBufStrings;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static io.datakernel.jmx.MBeanUtils.register;

public class HttpThrottlingServer {
	private static final String WOW = "wow";
	private static final String NIO = "nio";
	private static final Random rand = new Random();
	private static final int defaultLoadBusinessLogic = 0; // without load on the business logic
	private static final String TEST_RESPONSE = "Hello, World!";
	public static final int SERVER_PORT = 45566;

	static class ServerOptions {
		private int loadBusinessLogic;

		public ServerOptions(int loadBusinessLogic) {
			checkArgument(loadBusinessLogic >= 0);
			this.loadBusinessLogic = loadBusinessLogic;
		}

		public int getLoadBusinessLogic() {
			return loadBusinessLogic;
		}

		public static ServerOptions parseCommandLine(String[] args) {
			int loadBusinessLogic = defaultLoadBusinessLogic;
			for (int i = defaultLoadBusinessLogic; i < args.length; i++) {
				switch (args[i]) {
					case "-l":
						loadBusinessLogic = Integer.parseInt(args[++i]);
						break;
					case "-?":
					case "-h":
						usage();
						return null;
				}
			}
			return new ServerOptions(loadBusinessLogic);
		}

		public static void usage() {
			System.err.println(HttpThrottlingServer.class.getSimpleName() + " [options]\n" +
					"\t-l    - value of load server\n" +
					"\t-h/-? - this help.");
		}

		@Override
		public String toString() {
			return "Load business logic : " + getLoadBusinessLogic();
		}
	}

	private final AsyncHttpServer server;

	public HttpThrottlingServer(NioEventloop eventloop, ServerOptions options) {
		this.server = buildHttpServer(eventloop, options.getLoadBusinessLogic()).setListenPort(SERVER_PORT);
	}

	private static AsyncHttpServer buildHttpServer(NioEventloop eventloop, final int loadBusinessLogic) {
		eventloop.throttlingController = ThrottlingController.createDefaultThrottlingController();
//		final ByteBufPool byteBufferPool = new ByteBufPool(16, 65536);
		return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.onResult(longBusinessLogic(TEST_RESPONSE, loadBusinessLogic));
			}
		});
	}

	protected static HttpResponse longBusinessLogic(String response, int loadBusinessLogic) {
		long result = 0;
		for (int i = 0; i < loadBusinessLogic; ++i) {
			for (int j = 0; j < 200; ++j) {
				int index = Math.abs(rand.nextInt()) % response.length();
				result += response.charAt(index) * rand.nextLong();
			}
		}
		if (result % 3 != 0) {
			response += "!";
		}
		return HttpResponse.create().body(ByteBufStrings.encodeAscii(response));
	}

	public void start() throws Exception {
		server.listen();
	}

	public void stop() {
		server.close();
	}

	public static void info(ServerOptions options) {
		System.out.println("<" + HttpThrottlingServer.class.getSimpleName() + ">\n" + options.toString());
	}

	public static void main(String[] args) throws Exception {
		ServerOptions options = ServerOptions.parseCommandLine(args);
		if (options == null)
			return;
		info(options);

		final NioEventloop eventloop = new NioEventloop();

		final HttpThrottlingServer server = new HttpThrottlingServer(eventloop, options);

		// TODO(vmykhalko): reimplement jmx logic
//		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
//		eventloop.registerMBean(mbeanServer, WOW, NIO);
//		ByteBufPool.registerMBean(mbeanServer);
//		register(mbeanServer, MBeanFormat.name(WOW, NIO, ThrottlingController.class), eventloop.throttlingController);
		server.start();

		eventloop.run();
	}

}
