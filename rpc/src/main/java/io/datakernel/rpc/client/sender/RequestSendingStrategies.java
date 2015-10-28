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
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.BucketHashFunction;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

public final class RequestSendingStrategies {

	private RequestSendingStrategies()  {

	}

	public static RequestSenderToSingleServerFactory server(final InetSocketAddress address) {
		checkNotNull(address);
		return new RequestSenderToSingleServerFactory(address);
	}


	public static RequestSenderGroupFactory servers(InetSocketAddress ... addresses) {
		return servers(asList(addresses));
	}

	public static RequestSenderGroupFactory servers(final List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		return new RequestSenderGroupFactory(addresses);
	}


	public static RequestSenderToGroupFactory firstAvailable(RequestSenderFactory ... senders) {
		return firstAvailable(asList(senders));
	}

	public static RequestSenderToGroupFactory firstAvailable(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderToGroupFactory(senders) {
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


	public static RequestSenderToGroupFactory allAvailable(RequestSenderFactory ... senders) {
		return allAvailable(asList(senders));
	}


	public static RequestSenderToGroupFactory allAvailable(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderToGroupFactory(senders) {
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


	public static RequestSenderToGroupFactory roundRobin(RequestSenderFactory ... senders) {
		return roundRobin(asList(senders));
	}

	public static RequestSenderToGroupFactory roundRobin(final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		return new RequestSenderToGroupFactory(senders) {
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



	public static RequestSenderToGroupFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                                   RequestSenderFactory ... senders) {
		return sharding(hashFunction, asList(senders));
	}

	public static RequestSenderToGroupFactory sharding(final HashFunction<RpcMessageData> hashFunction,
	                                                   final List<RequestSenderFactory> senders) {
		checkNotNull(senders);
		checkArgument(senders.size() > 0, "at least one sender must be present");
		checkNotNull(hashFunction);
		return new RequestSenderToGroupFactory(senders) {
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



	public static RequestSenderFactoryWithKeys rendezvousHashing(final HashFunction<RpcMessageData> hashFunction) {
		checkNotNull(hashFunction);
		return new RequestSenderFactoryWithKeys(hashFunction);
	}



	public static RequestSenderDispatcherFactory typeDispatching() {
		return new RequestSenderDispatcherFactory();
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

	public static abstract class AbstractRequestSenderFactory implements RequestSenderFactory{
		protected abstract List<RequestSender> createAsList(RpcClientConnectionPool pool);
	}

	public static abstract class RequestSenderToGroupFactory extends AbstractRequestSenderFactory{
		private final List<RequestSenderFactory> subSenderFactories;

		private RequestSenderToGroupFactory(List<RequestSenderFactory> subSenderFactories) {
			this.subSenderFactories = subSenderFactories;
		}

		public final List<RequestSender> createSubSenders(RpcClientConnectionPool pool) {

			assert subSenderFactories != null;

			List<List<RequestSender>> listOfListOfSenders = new ArrayList<>();
			for (RequestSenderFactory subSenderFactory : subSenderFactories) {
				AbstractRequestSenderFactory abstractSubSenderFactory = (AbstractRequestSenderFactory)subSenderFactory;
				listOfListOfSenders.add(abstractSubSenderFactory.createAsList(pool));
			}
			return flatten(listOfListOfSenders);
		}
	}

	public static final class RequestSenderFactoryWithKeys extends AbstractRequestSenderFactory {

		private Map<Object, RequestSenderFactory> keyToFactory;
		private HashFunction<RpcMessageData> hashFunction;
		private BucketHashFunction bucketHashFunction;

		private RequestSenderFactoryWithKeys(HashFunction<RpcMessageData> hashFunction) {
			this(hashFunction, null);
		}

		private RequestSenderFactoryWithKeys(HashFunction<RpcMessageData> hashFunction,
		                                     BucketHashFunction bucketHashFunction) {
			this.keyToFactory = new HashMap<>();
			this.hashFunction = hashFunction;
			this.bucketHashFunction = bucketHashFunction;
		}

		@Override
		public RequestSender create(RpcClientConnectionPool pool) {
			Map<Object, RequestSender> keyToSender = new HashMap<>(keyToFactory.size());
			for (Object key : keyToFactory.keySet()) {
				keyToSender.put(key, keyToFactory.get(key).create(pool));
			}
			return bucketHashFunction != null ?
					new RequestSenderRendezvousHashing(keyToSender, hashFunction, bucketHashFunction) :
					new RequestSenderRendezvousHashing(keyToSender, hashFunction);
		}

		@Override
		protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
			return asList(create(pool));
		}



		// this group of similar methods was created to enable type checking
		// and ensure that servers() result can't be applied in put() method as second argument
		// because in this case we don't know how to choose one of them to send request

		public RequestSenderFactoryWithKeys put(Object key, RequestSenderToGroupFactory strategy) {
			return putCommon(key, strategy);
		}

		public RequestSenderFactoryWithKeys put(Object key, RequestSenderFactoryWithKeys strategy) {
			return putCommon(key, strategy);
		}

		public RequestSenderFactoryWithKeys put(Object key, RequestSenderToSingleServerFactory strategy) {
			return putCommon(key, strategy);
		}

		public RequestSenderFactoryWithKeys put(Object key, RequestSenderDispatcherFactory strategy) {
			return putCommon(key, strategy);
		}

		private RequestSenderFactoryWithKeys putCommon(Object key, RequestSenderFactory strategy) {
			checkNotNull(strategy);
			keyToFactory.put(key, strategy);
			return this;
		}
	}

	public static final class RequestSenderGroupFactory extends AbstractRequestSenderFactory {
		private List<InetSocketAddress> addresses;

		private RequestSenderGroupFactory(List<InetSocketAddress> addresses) {
			this.addresses = addresses;
		}

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
	}

	public static final class RequestSenderToSingleServerFactory extends AbstractRequestSenderFactory {
		private InetSocketAddress address;

		private RequestSenderToSingleServerFactory(InetSocketAddress address) {
			this.address = address;
		}

		@Override
		public RequestSender create(RpcClientConnectionPool pool) {
			return new RequestSenderToSingleServer(address, pool);
		}

		@Override
		protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
			return asList(create(pool));
		}
	}

	public static final class RequestSenderDispatcherFactory extends AbstractRequestSenderFactory {

		private Map<Class<? extends RpcMessageData>, RequestSenderFactory> dataTypeToFactory;
		private RequestSenderFactory defaultSenderFactory;

		private RequestSenderDispatcherFactory() {
			dataTypeToFactory = new HashMap<>();
			defaultSenderFactory = null;
		}

		@Override
		public RequestSender create(RpcClientConnectionPool pool) {
			Map<Class<? extends RpcMessageData>, RequestSender> dataTypeToSender
					= new HashMap<>(dataTypeToFactory.size());
			for (Class<? extends RpcMessageData> dataType : dataTypeToFactory.keySet()) {
				dataTypeToSender.put(dataType, dataTypeToFactory.get(dataType).create(pool));
			}
			return new RequestSenderTypeDispatcher(dataTypeToSender, defaultSenderFactory.create(pool));
		}

		@Override
		protected List<RequestSender> createAsList(RpcClientConnectionPool pool) {
			return asList(create(pool));
		}



		// this group of similar methods was created to enable type checking
		// and ensure that servers() result can't be applied in put() method as second argument
		// because in this case we don't know how to choose one of them to send request

		public RequestSenderDispatcherFactory on(Class<? extends RpcMessageData> dataType,
		                                         RequestSenderToGroupFactory strategy) {
			return onCommon(dataType, strategy);
		}

		public RequestSenderDispatcherFactory on(Class<? extends RpcMessageData> dataType,
		                                         RequestSenderFactoryWithKeys strategy) {
			return onCommon(dataType, strategy);
		}

		public RequestSenderDispatcherFactory on(Class<? extends RpcMessageData> dataType,
		                                         RequestSenderToSingleServerFactory strategy) {
			return onCommon(dataType, strategy);
		}

		public RequestSenderDispatcherFactory on(Class<? extends RpcMessageData> dataType,
		                                         RequestSenderDispatcherFactory strategy) {
			return onCommon(dataType, strategy);
		}

		private RequestSenderDispatcherFactory onCommon(Class<? extends RpcMessageData> dataType,
		                                                RequestSenderFactory strategy) {
			checkNotNull(dataType);
			checkNotNull(strategy);
			dataTypeToFactory.put(dataType, strategy);
			return this;
		}



		// this group of similar methods was created to enable type checking
		// and ensure that servers() result can't be applied in put() method as second argument
		// because in this case we don't know how to choose one of them to send request

		public RequestSenderDispatcherFactory onDefault(RequestSenderToGroupFactory strategy) {
			return onDefaultCommon(strategy);
		}

		public RequestSenderDispatcherFactory onDefault(RequestSenderFactoryWithKeys strategy) {
			return onDefaultCommon(strategy);
		}

		public RequestSenderDispatcherFactory onDefault(RequestSenderToSingleServerFactory strategy) {
			return onDefaultCommon(strategy);
		}

		public RequestSenderDispatcherFactory onDefault(RequestSenderDispatcherFactory strategy) {
			return onDefaultCommon(strategy);
		}

		private RequestSenderDispatcherFactory onDefaultCommon(RequestSenderFactory strategy) {
			checkState(defaultSenderFactory == null, "Default Sender is already set");
			defaultSenderFactory = strategy;
			return this;
		}
	}
}

