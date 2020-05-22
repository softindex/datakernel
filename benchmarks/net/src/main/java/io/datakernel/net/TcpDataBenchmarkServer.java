package io.datakernel.net;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.datastream.processor.StreamMapper;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.function.Function;

import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;

public class TcpDataBenchmarkServer extends Launcher {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	SimpleServer server(Eventloop eventloop) {
		return SimpleServer.create(eventloop,
				socket -> ChannelSupplier.ofSocket(socket)
						.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
						.transformWith(StreamMapper.create(Function.identity()))
						.transformWith(ChannelSerializer.create(INT_SERIALIZER))
						.streamTo(ChannelConsumer.ofSocket(socket)))
				.withListenPort(9001);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher server = new TcpDataBenchmarkServer();
		server.launch(args);
	}
}
