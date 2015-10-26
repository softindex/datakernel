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

import static io.datakernel.rpc.client.sender.RequestSenderUtils.EMPTY_KEY;

public abstract class RequestSenderFactory {

	private final List<? extends RequestSenderFactory> subSenderFactories;

	private RequestSenderFactory(List<? extends RequestSenderFactory> subSenderFactories) {
		this.subSenderFactories = subSenderFactories;
	}

	public abstract RequestSender create(RpcClientConnectionPool pool);
	protected abstract List<RequestSender> createAsList(RpcClientConnectionPool pool);

	protected final List<RequestSender> createSubSenders(RpcClientConnectionPool pool) {

		// method should be called only from factories that have subFactories
		assert subSenderFactories != null;

		List<List<RequestSender>> listOfListOfSenders = new ArrayList<>();
		for (RequestSenderFactory subSenderFactory : subSenderFactories) {
			listOfListOfSenders.add(subSenderFactory.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}



	public static RequestSenderFactory server(final InetSocketAddress address) {
		return server(EMPTY_KEY, address);
	}

	public static RequestSenderFactoryWithKey server(int key, final InetSocketAddress address) {
		checkNotNull(address);
		return new RequestSenderFactoryWithKey(null, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				return new RequestSenderToSingleServer(address, pool, getKeyForSender());
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
		return new RequestSenderFactoryWithKey(null, EMPTY_KEY) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				throw new UnsupportedOperationException();
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				List<RequestSender> senders = new ArrayList<>();
				for (InetSocketAddress address : addresses) {
					senders.add(new RequestSenderToSingleServer(address, pool, getKeyForSender()));
				}
				return senders;
			}
		};
	}




	public static RequestSenderFactoryWithKey firstAvailable(int key, RequestSenderFactory ... senders) {
		return (RequestSenderFactoryWithKey)firstAvailable(key, asList(senders));
	}

	public static RequestSenderFactory firstAvailable(RequestSenderFactory ... senders) {
		return firstAvailable(EMPTY_KEY, senders);
	}

	public static RequestSenderFactory firstAvailable(final List<RequestSenderFactory> senders) {
		return firstAvailable(EMPTY_KEY, senders);
	}

	public static RequestSenderFactoryWithKey firstAvailable(final int key, final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactoryWithKey(senders, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToFirst(subSenders, key);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}




	public static RequestSenderFactoryWithKey allAvailable(int key, RequestSenderFactory ... senders) {
		return (RequestSenderFactoryWithKey)allAvailable(key, asList(senders));
	}

	public static RequestSenderFactory allAvailable(RequestSenderFactory ... senders) {
		return allAvailable(EMPTY_KEY, senders);
	}

	public static RequestSenderFactory allAvailable(final List<RequestSenderFactory> senders) {
		return allAvailable(EMPTY_KEY, senders);
	}

	public static RequestSenderFactoryWithKey allAvailable(int key, final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactoryWithKey(senders, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderToAll(subSenders, getKeyForSender());
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}




	public static RequestSenderFactoryWithKey roundRobin(int key, RequestSenderFactory ... senders) {
		return (RequestSenderFactoryWithKey)roundRobin(key, asList(senders));
	}

	public static RequestSenderFactory roundRobin(RequestSenderFactory ... senders) {
		return roundRobin(EMPTY_KEY, senders);
	}

	public static RequestSenderFactory roundRobin(final List<RequestSenderFactory> senders) {
		return roundRobin(EMPTY_KEY, senders);
	}

	public static RequestSenderFactoryWithKey roundRobin(int key, final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderFactoryWithKey(senders, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRoundRobin(subSenders, getKeyForSender());
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}



	public static RequestSenderFactoryWithKey rendezvousHashing(int key, final HashFunction<RpcMessageData> hashFunction,
	                                                     RequestSenderFactoryWithKey ... senders) {
		return (RequestSenderFactoryWithKey)rendezvousHashing(key, hashFunction, asList(senders));
	}

	public static RequestSenderFactory rendezvousHashing(final HashFunction<RpcMessageData> hashFunction,
	                                                     RequestSenderFactoryWithKey ... senders) {
		return rendezvousHashing(EMPTY_KEY, hashFunction, senders);
	}

	public static RequestSenderFactory rendezvousHashing(final HashFunction<RpcMessageData> hashFunction,
	                                                     final List<RequestSenderFactoryWithKey> senders) {
		return rendezvousHashing(EMPTY_KEY, hashFunction, senders);
	}

	public static RequestSenderFactoryWithKey rendezvousHashing(int key, final HashFunction<RpcMessageData> hashFunction,
	                                                     final List<RequestSenderFactoryWithKey> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RequestSenderFactoryWithKey(senders, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderRendezvousHashing(subSenders, getKeyForSender(), hashFunction);
			}

			@Override
			protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
				return asList(create(pool));
			}
		};
	}


	public static RequestSenderFactoryWithKey sharding(int key, final HashFunction<RpcMessageData> hashFunction,
	                                            RequestSenderFactoryWithKey ... senders) {
		return (RequestSenderFactoryWithKey)sharding(key, hashFunction, asList(senders));
	}

	public static RequestSenderFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                            RequestSenderFactoryWithKey ... senders) {
		return sharding(EMPTY_KEY, hashFunction, senders);
	}

	public static RequestSenderFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                            final List<RequestSenderFactoryWithKey> senders) {
		return sharding(EMPTY_KEY, hashFunction, senders);
	}

	public static RequestSenderFactoryWithKey sharding(int key, final HashFunction<RpcMessageData> hashFunction,
	                                            final List<RequestSenderFactoryWithKey> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RequestSenderFactoryWithKey(senders, key) {
			@Override
			public RequestSender create(RpcClientConnectionPool pool) {
				List<RequestSender> subSenders = createSubSenders(pool);
				return new RequestSenderSharding(subSenders, getKeyForSender(), hashFunction);
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

	public static abstract class RequestSenderFactoryWithKey extends RequestSenderFactory {

		private int keyForSender;

		private RequestSenderFactoryWithKey(List<? extends RequestSenderFactory> subSenderFactories, int key) {
			super(subSenderFactories);
			this.keyForSender = key;
		}

		protected int getKeyForSender() {
			return keyForSender;
		}


	}
}
