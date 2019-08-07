import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceFactory;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

import java.util.Random;

public class InstanceFactoryExample {

	public static void main(String[] args) {
		Random random = new Random(System.currentTimeMillis());
		//[START REGION_1]
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(new Key<InstanceFactory<Integer>>() {});
			}

			@Provides
			Integer giveMe() {
				return random.nextInt(1000);
			}
		};
		//[END REGION_1]

		//[START REGION_2]
		Injector injector = Injector.of(cookbook);
		InstanceFactory<Integer> factory = injector.getInstance(new Key<InstanceFactory<Integer>>() {});
		Integer someInt = factory.create();
		Integer otherInt = factory.create();
		System.out.println("First : " + someInt + ", second one : " + otherInt);
		//[END REGION_2]
	}
}
