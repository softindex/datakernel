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

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.net.InetSocketAddress;
import java.util.List;

public class RequestSenders {

	private RequestSenders() {
	}

	public static List<RequestSender> serverGroup(List<InetSocketAddress>) {
		// TODO:
		return null;
	}

	public static RequestSender server(InetSocketAddress)


	public static RequestSenderFactory firstAvailable(List<RequestSender> senders) {
		return new RequestSenderFactory() {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderToFirst(pool);
			}
		};
	}

	public static RequestSenderFactory allAvailable() {
		return new RequestSenderFactory() {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderToAll(pool);
			}
		};
	}

	public static RequestSenderFactory roundRobin() {
		return new RequestSenderFactory() {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderRoundRobin(pool);
			}
		};
	}

	public static RequestSenderFactory consistentHashBucket(final HashFunction<RpcMessageData> hashFunction) {
		return new RequestSenderFactory() {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderRendezvousHashing(pool, hashFunction);
			}
		};
	}

	public static RequestSenderFactory sharding(final HashFunction<RpcMessageData> hashFunction) {
		return new RequestSenderFactory() {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderSharding(pool, hashFunction);
			}
		};
	}

}
