package org.example.client;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.serializer.SerializerBuilder;

import java.net.InetSocketAddress;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;

public class ConsoleRpcClient extends AbstractModule {
    private static final int RPC_LISTENER_PORT = 5353;

    @Provides
    Eventloop eventloop() {
        return Eventloop.create();
    }

    @Provides
    Config config() {
        return Config.create()
                .with("client.connectionPort", "5353")
                .overrideWith(ofProperties(System.getProperties()).getChild("config"));
    }

    @Provides
    @Export
    RpcClient rpcClient(Eventloop eventloop, Config config) {
        return RpcClient.create(eventloop)
                .withSerializerBuilder(SerializerBuilder.create(ClassLoader.getSystemClassLoader()))
                .withMessageTypes(String.class)
                .withStrategy(server(new InetSocketAddress(
                        config.get(ofInteger(), "client.connectionPort", RPC_LISTENER_PORT))));
    }
}
