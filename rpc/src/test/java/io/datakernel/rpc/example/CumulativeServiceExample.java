///*
// * Copyright (C) 2015 SoftIndex LLC.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.datakernel.rpc.example;
//
//import io.datakernel.async.CompletionCallback;
//import io.datakernel.async.ResultCallback;
//import io.datakernel.eventloop.NioEventloop;
//import io.datakernel.net.ConnectSettings;
//import io.datakernel.rpc.client.RpcClient;
//import io.datakernel.rpc.example.CumulativeServiceHelper.ValueMessage;
//import io.datakernel.rpc.server.RpcServer;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//
//import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
//import static java.util.Collections.singletonList;
//
///**
// * Here we construct and launch both server and client.
// */
//public class CumulativeServiceExample {
//	private static final int SERVICE_PORT = 12345;
//
//	public static void main(String[] args) throws IOException {
//		final NioEventloop eventloop = new NioEventloop();
//		final RpcServer server = CumulativeServiceHelper.createServer(eventloop, SERVICE_PORT);
//		final RpcClient client = CumulativeServiceHelper.createClient(eventloop, singletonList(new InetSocketAddress(SERVICE_PORT)), new ConnectSettings(500));
//
//		final CompletionCallback finishCallback = new CompletionCallback() {
//			@Override
//			public void onComplete() {
//				stopProcess();
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				System.err.println("Exception while process: " + exception);
//				stopProcess();
//			}
//
//			public void stopProcess() {
//				client.stop(ignoreCompletionCallback());
//				server.close();
//			}
//		};
//
//		final ResultCallback<ValueMessage> resultCallback = new ResultCallback<ValueMessage>() {
//			@Override
//			public void onResult(ValueMessage response) {
//				System.out.println("== CumulativeService response is: " + response.value);
//				finishCallback.onComplete();
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				finishCallback.onException(exception);
//			}
//		};
//
//		final CompletionCallback startClientComplete = new CompletionCallback() {
//			@Override
//			public void onComplete() {
//				eventloop.post(new Runnable() {
//					@Override
//					public void run() {
//						client.sendRequest(new ValueMessage(10), 1000, resultCallback);
//					}
//				});
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				finishCallback.onException(exception);
//			}
//		};
//
//		server.listen();
//		client.start(startClientComplete);
//		eventloop.run();
//	}
//}
