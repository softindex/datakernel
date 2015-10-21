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
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderSharding extends RequestSenderToGroup {
	private static final int HASH_BASE = 103;
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();
	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;

	public RequestSenderSharding(List<RequestSender> senders, HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		super(senders);
		this.hashFunction = checkNotNull(hashFunction);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, final ResultCallback<T> callback) {
		checkNotNull(callback);
		RequestSender sender = chooseSender(request);
		if (sender.isActive()) {
			sender.sendRequest(request, timeout, callback);
			return;
		}
		callback.onException(NO_AVAILABLE_CONNECTION);
	}

	private RequestSender chooseSender(RpcMessage.RpcMessageData request) {
		List<RequestSender> subSenders = getSubSenders();
		int index = Math.abs(hashFunction.hashCode(request)) % subSenders.size();
		return subSenders.get(index);
	}

	@Override
	protected int getHashBase() {
		return HASH_BASE;
	}
}
