import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

public class InstanceInjectorExample {

	//[START REGION_1]
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
	//[END REGION_1]

	public static void main(String[] args) {
		//[START REGION_2]
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
		//[END REGION_2]

		//[START REGION_3]
		Injector injector = Injector.of(cookbook);
		Butter butter = new Butter();
		InstanceInjector<Butter> instanceInjector = injector.getInstance(new Key<InstanceInjector<Butter>>() {});
		instanceInjector.injectInto(butter);

		System.out.println("After Inject : " + butter.getWeight());
		//[END REGION_3]
	}
}
