import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

public class InstanceInjectorExample {

	static class Butter {
		@Inject
		Float weight;

		@Inject
		String name;

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	public static void main(String[] args) {
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(new Key<InstanceInjector<Butter>>() {});
			}

			@Provides
			Float weight() { return 20.f; }

			@Provides
			String name() { return "Foreign Butter"; }
		};

		Injector injector = Injector.of(cookbook);
		Butter butter = new Butter();
		InstanceInjector<Butter> instanceInjector = injector.getInstance(new Key<InstanceInjector<Butter>>() {});
		instanceInjector.injectInto(butter);

		System.out.println("After Inject : " + butter.getWeight());
	}
}
