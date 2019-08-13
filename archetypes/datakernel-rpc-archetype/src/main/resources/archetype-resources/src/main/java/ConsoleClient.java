package org.example;

import io.datakernel.async.Callback;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;
import org.example.client.ConsoleRpcClient;

import java.util.Scanner;
import java.util.concurrent.CompletionStage;

public class ConsoleClient extends Launcher {

    @Inject
    private RpcClient client;

    private Callback<String> callback = (result, e) -> {
        if (e != null) {
            e.printStackTrace(System.err);
            return;
        }
        System.out.println("Response: " + result + "\n");
    };

    public static void main(String[] args) throws Exception {
        ConsoleClient client = new ConsoleClient();
        client.launch(args);
    }

    @Override
    protected Module getModule() {
        return Module.create()
                .install(ServiceGraphModule.create())
                .install(ConfigModule.create()
                        .printEffectiveConfig()
                        .rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}))
                .install(new ConsoleRpcClient());
    }

    @Override
    protected void run() throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String data = scanner.nextLine();
            System.out.println("Your input : " + data);
            if (data.equals("Exit")) {
                return;
            }
            client.sendRequest(data, callback);
        }
    }
}
