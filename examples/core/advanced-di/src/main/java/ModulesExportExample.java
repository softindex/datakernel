import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;

//[START EXAMPLE]
public class ModulesExportExample {
	public static void main(String[] args) {
		Injector injector = Injector.of(new AbstractModule() {
			@Provides
			String secretValue() { return "42"; }

			@Provides
			@Export
			Integer publicValue(String secretValue) { return Integer.parseInt(secretValue); }
		});
		Integer instance = injector.getInstance(Key.of(Integer.class));
		System.out.println(instance);
		String s = injector.getInstanceOrNull(Key.of(String.class));
		System.out.println("String is null : " + (s == null));
	}
}
//[END EXAMPLE]
