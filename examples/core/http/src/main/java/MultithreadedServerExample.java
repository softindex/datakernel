import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launchers.http.MultithreadedHttpServerLauncher;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * HTTP multithreaded server example.
 * Sends back a greeting and the number of worker which served the connection.
 */
public final class MultithreadedServerExample extends MultithreadedHttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Provides
	@Worker
	AsyncServlet servlet(@WorkerId int workerId) {
		return request -> Promise.of(
				HttpResponse.ok200()
						.withPlainText("Hello from worker server #" + workerId + "\n"));
	}

	public static void main(String[] args) throws Exception {
		MultithreadedServerExample example = new MultithreadedServerExample();
		example.launch(args);
	}
}
