package org.example.server;

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInteger;

public class MyRpcServer extends AbstractModule {
    private static final int RPC_SERVER_PORT = 5353;

    @Provides
    Config config() {
        return Config.create()
                .with("client.connectionPort", "5353")
                .overrideWith(ofProperties(System.getProperties()).getChild("config"));
    }

    @Provides
    @Export
    RpcServer provideRpcServer(Eventloop eventloop, Config config) {
        return RpcServer.create(eventloop)
                // You shouldn't forget about message serializer!
                .withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
                // You can define any message types by class
                .withMessageTypes(String.class)
                // Your message handlers can be written below
                .withHandler(String.class, String.class, request -> {
                    if (request.equals("Hello")) {
                        System.out.println("Hello comes");
                        return Promise.of("Hi, user!");
                    }
                    if (request.equals("What is your name?")) {
                        System.out.println("Name triggered");
                        return Promise.of("My name is ... RPC Server :)");
                    }
                    System.out.println("anyway");
                    return Promise.of(request + " " + request);
                })
                .withListenPort(config.get(ofInteger(), "client.connectionPort", RPC_SERVER_PORT));
    }
}
