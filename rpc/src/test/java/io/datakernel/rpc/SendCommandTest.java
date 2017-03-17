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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.client.sender.RpcStrategies;
import io.datakernel.rpc.server.RpcCommandHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SendCommandTest {
	private static final int PORT = 1234, TIMEOUT = 1500;

	private SampleCommandHandler commandHandler = new SampleCommandHandler();

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

	private RpcServer server = RpcServer.create(eventloop)
			.withMessageTypes(SampleCommand.class)
			.withHandler(SampleCommand.class, commandHandler)
			.withListenPort(PORT);
	;

	private RpcClient client = RpcClient.create(eventloop)
			.withStrategy(RpcStrategies.server(new InetSocketAddress(PORT)))
			.withMessageTypes(SampleCommand.class);
	;

	@Test
	public void testCommandsWithNoAnswer() throws Exception {
		server.listen();
		client.start(new CompletionCallback() {
			@Override
			protected void onComplete() {
				client.sendCommand(new SampleCommand("test-command"), TIMEOUT, new CompletionCallback() {
					@Override
					protected void onComplete() {
						System.out.println("Command was successfully sent");
						client.stop(IgnoreCompletionCallback.create());
						server.close(IgnoreCompletionCallback.create());
					}

					@Override
					protected void onException(Exception e) {
						fail();
					}
				});
			}

			@Override
			protected void onException(Exception e) {
				fail();
			}
		});

		eventloop.run();

		assertEquals("test-command", commandHandler.savedData);
	}

	public static class SampleCommand {
		@Serialize(order = 0)
		public String data;

		public SampleCommand(@Deserialize("data") String data) {
			this.data = data;
		}
	}

	static class SampleCommandHandler implements RpcCommandHandler<SampleCommand> {
		String savedData;

		@Override
		public void run(SampleCommand command, CompletionCallback callback) {
			savedData = command.data;
			callback.setComplete();
		}
	}
}
