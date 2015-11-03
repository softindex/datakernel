package io.datakernel.rpc.client.sender;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.BucketHashFunction;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

public final class RendezvousHashingStrategy extends AbstractRequestSendingStrategy{
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final BucketHashFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultBucketHashFunction();

	private final int minSubStrategiesForCreation;
	private final Map<Object, RequestSendingStrategy> keyToStrategy;
	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
	private final BucketHashFunction bucketHashFunction;

	private RendezvousHashingStrategy(HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		this(hashFunction, DEFAULT_BUCKET_HASH_FUNCTION);
	}

	private RendezvousHashingStrategy(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                                     BucketHashFunction bucketHashFunction) {
		this(hashFunction, bucketHashFunction, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT);
	}

	private RendezvousHashingStrategy(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                                  int minSubStrategiesForCreation) {
		this(hashFunction, DEFAULT_BUCKET_HASH_FUNCTION, minSubStrategiesForCreation);
	}

	private RendezvousHashingStrategy(HashFunction<RpcMessage.RpcMessageData> hashFunction,
	                                  BucketHashFunction bucketHashFunction,
	                                  int minSubStrategiesForCreation) {
		this.keyToStrategy = new HashMap<>();
		this.hashFunction = hashFunction;
		this.bucketHashFunction = bucketHashFunction;
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
	}


	// this group of similar methods was created to enable type checking
	// and ensure that servers() result can't be applied in put() method as second argument
	// because in this case we don't know how to choose one of them to send request

	public RendezvousHashingStrategy put(Object key, RequestSendingStrategyToGroup strategy) {
		return putCommon(key, strategy);
	}

	public RendezvousHashingStrategy put(Object key, RendezvousHashingStrategy strategy) {
		return putCommon(key, strategy);
	}

	public RendezvousHashingStrategy put(Object key, SingleServerStrategy strategy) {
		return putCommon(key, strategy);
	}

	public RendezvousHashingStrategy put(Object key, RequestSendingStrategies.RequestSenderDispatcherFactory strategy) {
		return putCommon(key, strategy);
	}

	private RendezvousHashingStrategy putCommon(Object key, RequestSendingStrategy strategy) {
		checkNotNull(strategy);
		keyToStrategy.put(key, strategy);
		return this;
	}




	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		Map<Object, Optional<RequestSender>> keyToSender = createKeyToSender(pool, keyToStrategy);
		if (countPresentValues(keyToSender) >= minSubStrategiesForCreation) {
			return Optional.<RequestSender>of(
					new RequestSenderRendezvousHashing(keyToSender, hashFunction, bucketHashFunction));
		} else {
			return Optional.absent();
		}
	}

	private static Map<Object, Optional<RequestSender>> createKeyToSender(RpcClientConnectionPool pool,
			Map<Object, RequestSendingStrategy> keyToStrategy) {

		assert keyToStrategy != null;

		Map<Object, Optional<RequestSender>> keyToSender = new HashMap<>();
		for (Object key : keyToStrategy.keySet()) {
			RequestSendingStrategy strategy = keyToStrategy.get(key);
			Optional<RequestSender> sender = strategy.create(pool);
			keyToSender.put(key, sender);
		}
		return keyToSender;
	}

	private static <T> int countPresentValues(Map<Object, Optional<T>> keyToSender) {
		int counter = 0;
		for (Optional<T> value : keyToSender.values()) {
			if (value.isPresent()) {
				++counter;
			}
		}
		return counter;
	}


	@VisibleForTesting
	final static class RequestSenderRendezvousHashing implements RequestSender {

		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashFunction<RpcMessage.RpcMessageData> hashFunction;
		final RendezvousHashBucket<RequestSender> hashBucket;


		public RequestSenderRendezvousHashing(Map<Object, Optional<RequestSender>> keyToSender,
		                                      HashFunction<RpcMessage.RpcMessageData> hashFunction,
		                                      BucketHashFunction bucketHashFunction) {
			checkNotNull(keyToSender);
			this.hashFunction = checkNotNull(hashFunction);
			this.hashBucket = new RendezvousHashBucket(keyToSender, bucketHashFunction);
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              final ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = getRequestSender(request);
			if (sender == null) {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
				return;
			}
			sender.sendRequest(request, timeout, callback);
		}

		private RequestSender getRequestSender(RpcMessage.RpcMessageData request) {
			int hash = hashFunction.hashCode(request);
			return hashBucket.chooseSender(hash);
		}
	}

	@VisibleForTesting
	static final class RendezvousHashBucket<T> {
		private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		private final Object[] baseHashes;
		private final BucketHashFunction bucketHashFunction;
		private final Map<Object, Optional<RequestSender>> keyToSender;

		public RendezvousHashBucket(Map<Object, Optional<RequestSender>> keyToSender,
		                            BucketHashFunction bucketHashFunction) {
			this(keyToSender, bucketHashFunction, DEFAULT_BUCKET_CAPACITY);
		}

		public RendezvousHashBucket(Map<Object, Optional<RequestSender>> keyToSender,
		                            BucketHashFunction bucketHashFunction, int capacity) {
			checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
			checkNotNull(bucketHashFunction);
			this.keyToSender = checkNotNull(keyToSender);
			this.baseHashes = new Object[capacity];
			this.bucketHashFunction = bucketHashFunction;
			computeBaseHashes();
		}

		// if activeAddresses is empty fill bucket -1
		private void computeBaseHashes() {
			for (int n = 0; n < baseHashes.length; n++) {
				Object senderKey = null;
				int max = Integer.MIN_VALUE;
				for (Object key : keyToSender.keySet()) {
					Optional<RequestSender> sender = keyToSender.get(key);
					if (sender.isPresent()) {
						int hash = bucketHashFunction.hash(key, n);
						if (hash >= max) {
							senderKey = key;
							max = hash;
						}
					}
				}
				baseHashes[n] = senderKey;
			}
		}

		public RequestSender chooseSender(int hash) {
			Object key = baseHashes[hash & (baseHashes.length - 1)];
			if (key != null) {
				assert keyToSender.get(key).isPresent();
				return keyToSender.get(key).get();
			} else {
				return null;
			}
		}
	}

	@VisibleForTesting
	static final class DefaultBucketHashFunction implements BucketHashFunction {
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		@Override
		public int hash(Object key, int bucket) {
			return murmurHashAddressFunction.newHasher()
					.putInt(key.hashCode())
					.putInt(bucket)
					.hash().asInt();
		}
	}
}
