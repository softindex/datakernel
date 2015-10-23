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
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderRoundRobin extends RequestSenderToGroup {
	private static final int HASH_BASE = 102;
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();

	private int nextSender;


	public RequestSenderRoundRobin(List<RequestSender> senders) {
		super(senders);
		nextSender = 0;
	}

	@Override
	public <T extends RpcMessageData> void sendRequest(RpcMessageData request, int timeout, ResultCallback<T> callback) {
		checkNotNull(callback);
		List<RequestSender> subSenders = getSubSenders();
		for (int i = 0; i < subSenders.size(); ++i) {
			RequestSender sender = subSenders.get(getCurrentSenderNumber());
			if (sender.isActive()) {
				sender.sendRequest(request, timeout, callback);
				return;
			}
		}
		callback.onException(NO_AVAILABLE_CONNECTION);
	}

	private int getCurrentSenderNumber() {
		// TODO (vmykhalko): maybe change subSenders to immutable list?
		int currentSender = nextSender;
		nextSender = (nextSender + 1) % getSubSenders().size();
		return currentSender;
	}

	@Override
	protected int getHashBase() {
		return HASH_BASE;
	}
}
