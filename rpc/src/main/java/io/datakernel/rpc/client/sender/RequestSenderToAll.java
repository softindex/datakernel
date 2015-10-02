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

package io.datakernel.rpc.client.sender;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;

import io.datakernel.async.FirstResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

final class RequestSenderToAll implements RequestSender {
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();
	private final RpcClientConnectionPool connections;

	// JMX
	private final long[] callCounters;

	public RequestSenderToAll(RpcClientConnectionPool connections) {
		this.connections = checkNotNull(connections);

		this.callCounters = new long[connections.addresses().size()];
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, final ResultCallback<T> callback) {
		checkNotNull(callback);
		int serverNumber = 0;
		int calls = 0;
		FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
		for (InetSocketAddress address : connections.addresses()) {
			RpcClientConnection connection = connections.get(address);
			if (connection == null) {
				serverNumber++;
				continue;
			}
			++calls;
			++callCounters[serverNumber];
			connection.callMethod(request, timeout, resultCallback);
		}
		if (calls == 0) {
			callback.onException(NO_AVAILABLE_CONNECTION);
		} else {
			resultCallback.resultOf(calls);
		}
	}

	@Override
	public void onConnectionsUpdated() {
	}

	@Override
	public void resetStats() {
		for (int i = 0; i < callCounters.length; i++) {
			callCounters[i] = 0;
		}
	}

	@Override
	public CompositeData getRequestSenderInfo() throws OpenDataException {
		List<String> res = new ArrayList<>();
		res.add("address;calls");
		for (int i = 0; i < connections.addresses().size(); i++) {
			res.add(connections.addresses().get(i) + ";" + callCounters[i]);
		}
		return CompositeDataBuilder.builder(RequestSenderToAll.class.getSimpleName())
				.add("senderStrategy", SimpleType.STRING, RequestSenderToAll.class.getSimpleName())
				.add("callsPerAddress", new ArrayType<>(1, SimpleType.STRING), res.toArray(new String[res.size()]))
				.build();
	}
}
