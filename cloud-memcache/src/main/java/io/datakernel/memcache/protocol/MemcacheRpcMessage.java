package io.datakernel.memcache.protocol;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMandatoryData;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;

import java.util.Arrays;
import java.util.List;

public class MemcacheRpcMessage {
	public static final HashFunction<Object> HASH_FUNCTION =
			item -> {
				if (item instanceof GetRequest) {
					GetRequest request = (GetRequest) item;
					return Arrays.hashCode(request.getKey());
				} else if (item instanceof PutRequest) {
					PutRequest request = (PutRequest) item;
					return Arrays.hashCode(request.getKey());
				}
				throw new IllegalArgumentException("Unknown request type " + item);
			};

	public static final List<Class<?>> MESSAGE_TYPES = Arrays.asList(GetRequest.class, GetResponse.class, PutRequest.class, PutResponse.class);

	public static final class GetRequest implements RpcMandatoryData {
		private final byte[] key;

		public GetRequest(@Deserialize("key") byte[] key) {
			this.key = key;
		}

		@Serialize(order = 1)
		public byte[] getKey() {
			return key;
		}
	}

	public static final class GetResponse {
		private final ByteBuf data;

		public GetResponse(@Deserialize("data") ByteBuf data) {
			this.data = data;
		}

		@Serialize(order = 1)
		@SerializeNullable
		public ByteBuf getData() {
			return data;
		}
	}

	public static final class PutRequest {
		private final byte[] key;
		private final ByteBuf data;

		public PutRequest(@Deserialize("key") byte[] key, @Deserialize("data") ByteBuf data) {
			this.key = key;
			this.data = data;
		}

		@Serialize(order = 1)
		public byte[] getKey() {
			return key;
		}

		@SerializeNullable
		@Serialize(order = 2)
		public ByteBuf getData() {
			return data;
		}
	}

	public static final class PutResponse {
		public static final PutResponse INSTANCE = new PutResponse();
	}

}
