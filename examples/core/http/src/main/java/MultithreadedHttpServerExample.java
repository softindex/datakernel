import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launchers.http.MultithreadedHttpServerLauncher;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

/**
 * HTTP multithreaded server example.
 * Sends back a greeting and the number of worker which served the connection.
 */
//[START EXAMPLE]
public final class MultithreadedHttpServerExample extends MultithreadedHttpServerLauncher {
	@Provides
	@Worker
	AsyncServlet servlet(@WorkerId int workerId) {
		return request -> HttpResponse.ok200()
				.withPlainText("Hello from worker server #" + workerId + "\n");
	}

	public static void main(String[] args) throws Exception {
		MultithreadedHttpServerExample example = new MultithreadedHttpServerExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
