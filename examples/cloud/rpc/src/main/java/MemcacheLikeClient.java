import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.memcache.client.MemcacheClient;
import io.datakernel.memcache.client.MemcacheClientModule;
import io.datakernel.memcache.protocol.MemcacheRpcMessage;
import io.datakernel.memcache.protocol.SerializerGenByteBuf;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.memcache.protocol.MemcacheRpcMessage.HASH_FUNCTION;
import static io.datakernel.rpc.client.sender.RpcStrategies.rendezvousHashing;
import static io.datakernel.util.MemSize.bytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 06.08.19.
 */
public class MemcacheLikeClient extends Launcher {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.packetSize", "64")
				.with("protocol.packetSizeMax", "64")
				.with("protocol.compression", "false")
				.with("server.listenAddresses", "localhost:8080")
				.with("client.addresses", "localhost:8080")
				.overrideWith(Config.ofProperties(System.getProperties()));
	}

	@Provides
	RpcClient rpcClient(Config config, Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withStrategy(rendezvousHashing(HASH_FUNCTION)
						.withMinActiveShards(config.get(ofInteger(), "client.minAliveConnections", 1))
						.withShards(new InetSocketAddress(config.get(ofInteger(), "client.addresses"))))
				.withMessageTypes(MemcacheRpcMessage.MESSAGE_TYPES)
				.withSerializerBuilder(SerializerBuilder.create(ClassLoader.getSystemClassLoader())
						.withSerializer(ByteBuf.class, new SerializerGenByteBuf(false)))
				.withStreamProtocol(
						config.get(ofMemSize(), "protocol.packetSize", bytes(64)),
						config.get(ofMemSize(), "protocol.packetSizeMax", bytes(128)),
						config.get(ofBoolean(), "protocol.compression", false))
				.withLogger(getLogger(MemcacheClient.class));

	}

	@Inject
	MemcacheClient client;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new MemcacheClientModule());
	}

	@Override
	protected void run() throws Exception {
		for (int i = 0; i < 10; ++i) {
			Promise<Void> put = client.put(ByteBuffer.allocate(4).putInt(i).array(), ByteBuf.wrapForWriting(("Miles -> " + i).getBytes(UTF_8)), 5);
			put.whenComplete((res, e) -> System.out.println("Request sent"));
		}

		for(int j = 0; j < 10; ++j) {
			Promise<ByteBuf> byteBufPromise = client.get(ByteBuffer.allocate(4).putInt(j).array(), 10);
			byteBufPromise.whenResult(res -> System.out.println("Got back " + res.toString()));
		}
		eventloop.run();
	}

	public static void main(String[] args) throws Exception {
		Launcher client = new MemcacheLikeClient();
		client.launch(args);
	}
}
