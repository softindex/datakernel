import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.launcher.Launcher;

@SuppressWarnings("FinalClass")
public final class InstanceInjectorExample extends Launcher {
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
		Launcher launcher = new InstanceInjectorExample();
		launcher.launch(args);
	}

	// internal job of post-creation objects inject.
	private void postInjectInstances(String [] args) {
		Injector injector = this.createInjector(args);
		InstanceInjector<Launcher> instanceInjector = injector.getInstanceInjector(Launcher.class);
		instanceInjector.injectInto(this);
	}

}
