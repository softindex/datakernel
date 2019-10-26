package memcached;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.memcache.protocol.SerializerDefSlice;
import io.datakernel.memcache.server.RingBuffer;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

import java.net.InetSocketAddress;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofMemSize;
import static io.datakernel.memcache.protocol.MemcacheRpcMessage.*;

public class MemcacheMultiServerModule extends AbstractModule {
	private MemcacheMultiServerModule() {}

	public static MemcacheMultiServerModule create() {
		return new MemcacheMultiServerModule();
	}

	@Provides
	@Worker
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	InetSocketAddress port(@WorkerId int workerId) {
		return new InetSocketAddress("localhost", 9000 + workerId);
	}

	@Provides
	@Worker
	RingBuffer ringBuffer(Config config) {
		return RingBuffer.create(
				config.get(ofInteger(), "memcache.buffers"),
				config.get(ofMemSize(), "memcache.bufferCapacity").toInt());
	}

	@Provides
	@Worker
	RpcServer server(Eventloop eventloop, RingBuffer storage, InetSocketAddress address) {
		return RpcServer.create(eventloop)
				.withHandler(GetRequest.class, GetResponse.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class, PutResponse.class,
						request -> {
							Slice slice = request.getData();
							System.out.println("Server on port #" + address.getPort() + " accepted message!");
							storage.put(request.getKey(), slice.array(), slice.offset(), slice.length());
							return Promise.of(PutResponse.INSTANCE);
						})
				.withSerializerBuilder(SerializerBuilder.create(ClassLoader.getSystemClassLoader())
						.withSerializer(Slice.class, new SerializerDefSlice()))
				.withMessageTypes(MESSAGE_TYPES)
				.withListenAddresses(address);
	}
}
