import io.datakernel.di.Injector;
import io.datakernel.di.InstanceInjector;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.launcher.Launcher;

public final class InstanceInjectorExample extends Launcher {
	//[START REGION_1]
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
	//[END REGION_1]

	// internal job of post-creation objects inject.
	//[START REGION_2]
	private void postInjectInstances(String[] args) {
		Injector injector = this.createInjector(args);
		InstanceInjector<Launcher> instanceInjector = injector.getInstanceInjector(Launcher.class);
		instanceInjector.injectInto(this);
	}
	//[END REGION_2]

}
