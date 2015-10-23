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

import com.google.common.annotations.VisibleForTesting;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;

public abstract class RequestSenderFactory {

	private final List<RequestSenderFactory> subSenderFactories;

	public RequestSenderFactory(List<RequestSenderFactory> subSenderFactories) {
		this.subSenderFactories = subSenderFactories;
	}

	public abstract RequestSender create(RpcClientConnectionPool pool);
	protected abstract List<RequestSender> createAsList(RpcClientConnectionPool pool);

	protected List<RequestSender> createSubSenders(RpcClientConnectionPool pool) {

		// method should be called only from factories that have subFactories
		assert subSenderFactories != null;

		List<List<RequestSender>> listOfListOfSenders = new ArrayList<>();
		for (RequestSenderFactory subSenderFactory : subSenderFactories) {
			listOfListOfSenders.add(subSenderFactory.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}

	public static RequestSenderFactory server(final InetSocketAddress address) {
		checkNotNull(address);
		return new RequestSenderFactory(null) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderToSingleServer(address, pool);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}


	public static RequestSenderFactory servers(InetSocketAddress ... addresses) {
		return servers(asList(addresses));
	}

	public static RequestSenderFactory servers(final List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		return new RequestSenderFactory(null) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				throw new UnsupportedOperationException();
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				List<RequestSender> senders = new ArrayList<>();
				for (InetSocketAddress address : addresses) {
					senders.add(new RequestSenderToSingleServer(address, pool));
				}
				return senders;
			}
		};
	}


	public static RequestSenderFactory firstAvailable(RequestSenderFactory ... senders) {
		return firstAvailable(asList(senders));
	}

	public static RequestSenderFactory firstAvailable(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToFirst(subSenders);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}


	public static RequestSenderFactory allAvailable(RequestSenderFactory ... senders) {
		return allAvailable(asList(senders));
	}


	public static RequestSenderFactory allAvailable(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToAll(subSenders);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}


	public static RequestSenderFactory roundRobin(RequestSenderFactory ... senders) {
		return roundRobin(asList(senders));
	}

	public static RequestSenderFactory roundRobin(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRoundRobin(subSenders);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}

	public static RequestSenderFactory rendezvousHashing(final HashFunction<RpcMessageData> hashFunction,
	                                                     RequestSenderFactory ... senders) {
		return rendezvousHashing(hashFunction, asList(senders));
	}

	public static RequestSenderFactory rendezvousHashing(final HashFunction<RpcMessageData> hashFunction,
	                                                     final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRendezvousHashing(subSenders, hashFunction);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}


	public static RequestSenderFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                            RequestSenderFactory ... senders) {
		return sharding(hashFunction, asList(senders));
	}

	public static RequestSenderFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                            final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RequestSenderFactory(senders) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderSharding(subSenders, hashFunction);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}

	@VisibleForTesting
	static <T> List<T> flatten(List<List<T>> listOfList) {
		List<T> flatList = new ArrayList<>();
		for (List<T> list : listOfList) {
			for (T element : list) {
				flatList.add(element);
			}
		}
		return flatList;
	}
}
