import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

import java.util.Random;

public class InstanceProviderExample {

	public static void main(String[] args) {
		Random random = new Random(System.currentTimeMillis());
		//[START REGION_1]
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(new Key<InstanceProvider<Integer>>() {});
			}

			@Provides
			Integer giveMe() {
				return random.nextInt(1000);
			}
		};
		//[END REGION_1]

		//[START REGION_2]
		Injector injector = Injector.of(cookbook);
		InstanceProvider<Integer> provider = injector.getInstance(new Key<InstanceProvider<Integer>>() {});
		// lazy value get.
		Integer someInt = provider.get();
		System.out.println(someInt);
		//[END REGION_2]
	}
}
