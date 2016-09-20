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

import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.DynamicMBeanFactory;
import io.datakernel.jmx.JmxMBeans;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Random;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public class HttpThrottlingServer {
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

	public HttpThrottlingServer(Eventloop eventloop, ServerOptions options) {
		this.server = buildHttpServer(eventloop, options.getLoadBusinessLogic());
	}

	private static AsyncHttpServer buildHttpServer(Eventloop eventloop, final int loadBusinessLogic) {
//		final ByteBufPool byteBufferPool = new ByteBufPool(16, 65536);
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				callback.onResult(longBusinessLogic(TEST_RESPONSE, loadBusinessLogic));
			}
		};

		return AsyncHttpServer.create(eventloop, servlet).withListenPort(SERVER_PORT);
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
		return HttpResponse.ok200().withBody(ByteBufStrings.encodeAscii(response));
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

		final Eventloop eventloop = Eventloop.create();
		ThrottlingController throttlingController = ThrottlingController.createDefaultThrottlingController(eventloop);

		final HttpThrottlingServer server = new HttpThrottlingServer(eventloop, options);

		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		DynamicMBeanFactory mBeanFactory = JmxMBeans.factory();
		mbeanServer.registerMBean(mBeanFactory.createFor(asList(eventloop), true),
				new ObjectName(Eventloop.class.getPackage().getName() + ":type=Eventloop"));
		mbeanServer.registerMBean(ByteBufPool.getStats(),
				new ObjectName(ByteBufPool.class.getPackage().getName() + ":type=ByteBufPool"));
		mbeanServer.registerMBean(mBeanFactory.createFor(asList(throttlingController), true),
				new ObjectName(ThrottlingController.class.getPackage().getName() + ":type=ThrottlingController"));
		server.start();

		eventloop.run();
	}

}
