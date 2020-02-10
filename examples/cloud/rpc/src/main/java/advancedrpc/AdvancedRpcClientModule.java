package advancedrpc;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.rpc.client.sender.RpcStrategies.*;

public class AdvancedRpcClientModule extends AbstractModule {
	private AdvancedRpcClientModule() {
	}

	public static AdvancedRpcClientModule create() {
		return new AdvancedRpcClientModule();
	}

	@Provides
	RpcClient rpcClient(Eventloop eventloop, RpcStrategy strategy) {
		return RpcClient.create(eventloop)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(Integer.class)
				.withStrategy(strategy);
	}

	@Provides
	RpcStrategy rpcStrategy(Config config) {
		List<InetSocketAddress> inetAddresses = config.get(ConfigConverters.ofList(
				ConfigConverters.ofInetSocketAddress(), ","), "client.addresses");
		checkState(inetAddresses.size() == 4);

		return firstAvailable(
				rendezvousHashing(Object::hashCode)
						.withShard(1, server(inetAddresses.get(0)))
						.withShard(2, server(inetAddresses.get(1))),
				rendezvousHashing(Object::hashCode)
						.withShard(1, server(inetAddresses.get(2)))
						.withShard(2, server(inetAddresses.get(3)))
		);
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.compression", "false")
				.with("client.addresses", "localhost:9000, localhost:9001, localhost:9002, localhost:9003")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}
}
