import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

//[START EXAMPLE]
public class ModulesExportExample {
	final static AbstractModule module = new AbstractModule() {
		@Provides
		String secretValue() { return "420"; }

		@Provides
		@Export
		Integer publicValue() { return 42; }
	};

	public static void main(String[] args) {
		Injector injector = Injector.of(module);
		Integer instance = injector.getInstance(Key.of(Integer.class));
		System.out.println(instance);
		String s = injector.getInstanceOrNull(Key.of(String.class));
		System.out.println("String is null : " + (s == null));
	}
}
//[END EXAMPLE]
