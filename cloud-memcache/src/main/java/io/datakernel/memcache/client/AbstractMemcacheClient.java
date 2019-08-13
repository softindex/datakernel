package io.datakernel.memcache.client;

import io.datakernel.async.Promise;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.GetRequest;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.GetResponse;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.PutRequest;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.rpc.client.IRpcClient;

public abstract class AbstractMemcacheClient<K, V> implements MemcacheClient<K, V> {
	private final IRpcClient rpcClient;

	protected AbstractMemcacheClient(IRpcClient rpcClient) {
		this.rpcClient = rpcClient;
	}

	protected abstract byte[] encodeKey(K key);

	protected abstract Slice encodeValue(V value);

	protected abstract V decodeValue(Slice slice);

	@Override
	public Promise<Void> put(K key, V value, int timeout) {
		PutRequest request = new PutRequest(encodeKey(key), encodeValue(value));
		return rpcClient.sendRequest(request, timeout).toVoid();
	}

	@Override
	public Promise<V> get(K key, int timeout) {
		GetRequest request = new GetRequest(encodeKey(key));
		return rpcClient.<GetRequest, GetResponse>sendRequest(request, timeout)
				.map(response -> decodeValue(response.getData()));
	}

	@Override
	public Promise<Void> put(K key, V value) {
		PutRequest request = new PutRequest(encodeKey(key), encodeValue(value));
		return rpcClient.sendRequest(request).toVoid();
	}

	@Override
	public Promise<V> get(K key) {
		GetRequest request = new GetRequest(encodeKey(key));
		return rpcClient.<GetRequest, GetResponse>sendRequest(request)
				.map(response -> decodeValue(response.getData()));
	}
}
