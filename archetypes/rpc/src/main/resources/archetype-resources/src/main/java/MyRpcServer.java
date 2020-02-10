package ${groupId};

import io.datakernel.promise.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launchers.rpc.RpcServerLauncher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

@SuppressWarnings("unused")
public class MyRpcServer extends RpcServerLauncher {
    private static final int RPC_SERVER_PORT = 5353;

    @Provides
    RpcServer provideRpcServer(Eventloop eventloop, Config config) {
        return RpcServer.create(eventloop)
                // You shouldn't forget about message serializer!
                .withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
                // You can define any message types by class
                .withMessageTypes(String.class)
                // Your message handlers can be written below
                .withHandler(String.class, String.class, request -> {
                    if (request.toLowerCase().equals("hello") || request.toLowerCase().equals("hi")) {
                        return Promise.of("Hi, user!");
                    }
                    if (request.equals("What is your name?")) {
                        return Promise.of("My name is ... RPC Server :)");
                    }
                    return Promise.of(request + " " + request);
                })
                .withListenPort(config.get(ConfigConverters.ofInteger(), "client.connectionPort", RPC_SERVER_PORT));
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    public static void main(String[] args) throws Exception {
        MyRpcServer rpcApp = new MyRpcServer();
        rpcApp.launch(args);
    }
}
