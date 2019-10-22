package io.datakernel.memcache.server;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.memcache.protocol.SerializerGenSlice;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.common.MemSize.kilobytes;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.memcache.protocol.MemcacheRpcMessage.*;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_SERVER_SOCKET_SETTINGS;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_SOCKET_SETTINGS;

public class MemcacheServerModule extends AbstractModule {
	private MemcacheServerModule() {}

	public static MemcacheServerModule create() {
		return new MemcacheServerModule();
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RingBuffer ringBuffer(Config config) {
		return RingBuffer.create(
				config.get(ofInteger(), "memcache.buffers"),
				config.get(ofMemSize(), "memcache.bufferCapacity").toInt());
	}

	@Provides
	@Export
	RpcServer server(Eventloop eventloop, Config config, RingBuffer storage) {
		return RpcServer.create(eventloop)
				.withHandler(GetRequest.class, GetResponse.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class, PutResponse.class,
						request -> {
							Slice slice = request.getData();
							storage.put(request.getKey(), slice.array(), slice.offset(), slice.length());
							return Promise.of(PutResponse.INSTANCE);
						})
				.withSerializerBuilder(SerializerBuilder.create(ClassLoader.getSystemClassLoader())
						.withSerializer(Slice.class, new SerializerGenSlice()))
				.withMessageTypes(MESSAGE_TYPES)
				.withStreamProtocol(
						config.get(ofMemSize(), "protocol.packetSize", kilobytes(64)),
						config.get(ofMemSize(), "protocol.packetSizeMax", kilobytes(64)),
						config.get(ofBoolean(), "protocol.compression", false))
				.withServerSocketSettings(config.get(ofServerSocketSettings(), "server.serverSocketSettings", DEFAULT_SERVER_SOCKET_SETTINGS))
				.withSocketSettings(config.get(ofSocketSettings(), "server.socketSettings", DEFAULT_SOCKET_SETTINGS))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "server.listenAddresses"));
	}
}
