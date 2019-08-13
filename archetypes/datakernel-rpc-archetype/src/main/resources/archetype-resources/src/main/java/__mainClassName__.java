package ${groupId};

import io.datakernel.di.module.Module;
import io.datakernel.launchers.rpc.RpcServerLauncher;
import org.example.server.MyRpcServer;

public class ${mainClassName} extends RpcServerLauncher {
    @Override
    protected Module getBusinessLogicModule() {
        return Module.create()
                .install(new MyRpcServer());
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    public static void main(String[] args) throws Exception {
		${mainClassName} rpcApp = new ${mainClassName}();
        rpcApp.launch(args);
    }
}
