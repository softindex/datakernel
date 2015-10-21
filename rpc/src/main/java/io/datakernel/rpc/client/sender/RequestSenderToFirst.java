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

import io.datakernel.async.ResultCallback;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderToFirst extends RequestSenderToGroup {
	private static final int HASH_BASE = 101;
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();

	public RequestSenderToFirst(List<RequestSender> senders) {
		super(senders);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
		checkNotNull(callback);
		for (RequestSender subSender : getSubSenders()) {
			if (!subSender.isActive()) {
				continue;
			}
			// TODO (vmykhalko): what if connection was removed from pool concurrently at this moment?
			subSender.sendRequest(request, timeout, callback);
			return;
		}
		callback.onException(NO_AVAILABLE_CONNECTION);
	}

	@Override
	protected int getHashBase() {
		return HASH_BASE;
	}
}
