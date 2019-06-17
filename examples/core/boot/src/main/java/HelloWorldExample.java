import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.launcher.Launcher;

public final class HelloWorldExample extends Launcher {
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
