import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.launcher.Launcher;
import org.slf4j.bridge.SLF4JBridgeHandler;

public final class HelloWorldExample extends Launcher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Inject
	String message;

	@Provides
	String message() {
		return "Hello, world!";
	}

	@Override
	protected void run() {
		System.out.println(message);
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HelloWorldExample();
		launcher.launch(args);
	}
}
