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

package io.datakernel.rpc.client;

import com.google.common.base.Stopwatch;
import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.DynamicStatsCounter;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.StatsCounter;
import io.datakernel.rpc.protocol.*;
import org.slf4j.Logger;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

public interface RpcClientConnection extends RpcConnection, RpcClientConnectionMBean {

	<I extends RpcMessage.RpcMessageData, O extends RpcMessage.RpcMessageData> void callMethod(
			I request, int timeout, ResultCallback<O> callback);

	void close();

	SocketConnection getSocketConnection();

	interface StatusListener {
		void onOpen(RpcClientConnection connection);

		void onClosed();
	}
}
