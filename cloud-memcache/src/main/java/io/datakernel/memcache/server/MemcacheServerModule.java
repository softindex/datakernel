package io.datakernel.memcache.server;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.server.RpcServer;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.memcache.protocol.MemcacheRpcMessage.*;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_SERVER_SOCKET_SETTINGS;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_SOCKET_SETTINGS;
import static io.datakernel.util.MemSize.kilobytes;

public class MemcacheServerModule extends AbstractModule {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RingBuffer ringBuffer(Eventloop eventloop, Config config) {
		return RingBuffer.create(
				config.get(ofInteger(), "memcache.buffers"),
				config.get(ofMemSize(), "memcache.bufferCapacity").toInt());
	}

	@Provides
	RpcServer server(Eventloop eventloop, Config config, RingBuffer storage) {
		return RpcServer.create(eventloop)
				.withHandler(GetRequest.class, GetResponse.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class, PutResponse.class,
						request -> {
							ByteBuf buf = request.getData();
							storage.put(request.getKey(), buf.array(), buf.head(), buf.readRemaining());
							buf.recycle();
							return Promise.of(PutResponse.INSTANCE);
						})
				.withMessageTypes(MESSAGE_TYPES)
				.withStreamProtocol(
						config.get(ofMemSize(), "protocol.packetSize", kilobytes(64)),
						config.get(ofMemSize(), "protocol.packetSizeMax", kilobytes(64)),
						config.get(ofBoolean(), "protocol.compression", false))
				.withServerSocketSettings(config.get(ofServerSocketSettings(), "server.serverSocketSettings", DEFAULT_SERVER_SOCKET_SETTINGS))
				.withSocketSettings(config.get(ofSocketSettings(), "server.socketSettings", DEFAULT_SOCKET_SETTINGS))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "server.listenAddresses"))
				.withAutoFlushInterval(config.get(ofDuration(), "server.flushDelayMillis", Duration.ZERO));
	}
}
