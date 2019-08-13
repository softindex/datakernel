package io.datakernel.memcache.client;

import io.datakernel.async.Promise;

public interface MemcacheClient {
	final class Slice {
		private final byte[] array;
		private final int offset;
		private final int length;

		public Slice(byte[] array) {
			this.array = array;
			this.offset = 0;
			this.length = array.length;
		}

		public Slice(byte[] array, int offset, int length) {
			this.array = array;
			this.offset = offset;
			this.length = length;
		}

		public byte[] array() {
			return array;
		}

		public int offset() {
			return offset;
		}

		public int length() {
			return length;
		}
	}

	Promise<Void> put(byte[] key, Slice buf, int timeout);

	Promise<Slice> get(byte[] key, int timeout);

	default Promise<Void> put(byte[] key, Slice buf) {
		return put(key, buf, Integer.MAX_VALUE);
	}

	default Promise<Slice> get(byte[] key) {
		return get(key, Integer.MAX_VALUE);
	}
}
