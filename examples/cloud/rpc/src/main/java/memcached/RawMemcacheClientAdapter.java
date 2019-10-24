package memcached;

import io.datakernel.memcache.client.RawMemcacheClient;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.promise.Promise;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RawMemcacheClientAdapter {
	private RawMemcacheClient client;

	RawMemcacheClientAdapter(RawMemcacheClient client) {
		this.client = client;
	}

	public Promise<Void> put(int key, String data) {
		return client.put(new byte[]{(byte) key}, new Slice(data.getBytes()));
	}

	public Promise<String> get(int key) {
		return client.get(new byte[]{(byte) key}).map(this::decodeSlice);
	}

	private String decodeSlice(Slice slice) {
		return new String(slice.array(), slice.offset(), slice.length(), UTF_8);
	}
}
