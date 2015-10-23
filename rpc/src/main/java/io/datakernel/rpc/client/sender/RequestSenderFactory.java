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

import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class RequestSenderFactory {

	private final List<RequestSenderFactory> subSenderFactories;

	public RequestSenderFactory(List<RequestSenderFactory> subSenderFactories) {
		this.subSenderFactories = subSenderFactories;
	}

	public abstract RequestSender create(RpcClientConnectionPool pool);

	protected List<RequestSender> createSubSenders(RpcClientConnectionPool pool) {

		// method should be called only from factories that have subFactories
		assert subSenderFactories != null;

		List<RequestSender> requestSenders = new ArrayList<>(subSenderFactories.size());
		for (RequestSenderFactory subSenderFactory : subSenderFactories) {
			requestSenders.add(subSenderFactory.create(pool));
		}
		return requestSenders;
	}

//	public static List<RequestSender> serverGroup(List<InetSocketAddress>) {
//		// TODO:
//		return null;
//	}

	public static RequestSenderFactory server(final InetSocketAddress address) {
		checkNotNull(address);
		return new RequestSenderFactory(null) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderToSingleServer(address, pool);
			}
		};
	}

//	public static RequestSenderFactory servers()


	public static RequestSenderFactory firstAvailable(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToFirst(subSenders);
			}
		};
	}

	public static RequestSenderFactory allAvailable(final List<RequestSenderFactory> senders) {
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToAll(subSenders);
			}
		};
	}

	public static RequestSenderFactory roundRobin(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRoundRobin(subSenders);
			}
		};
	}

	public static RequestSenderFactory rendezvousHashing(final List<RequestSenderFactory> senders,
	                                                     final HashFunction<RpcMessageData> hashFunction) {
		checkNotNull(senders);
		checkNotNull(hashFunction);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRendezvousHashing(subSenders, hashFunction);
			}
		};
	}

	public static RequestSenderFactory sharding(final List<RequestSenderFactory> senders,
	                                            final HashFunction<RpcMessageData> hashFunction) {
		checkNotNull(senders);
		checkNotNull(hashFunction);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderSharding(subSenders, hashFunction);
			}
		};
	}
}
