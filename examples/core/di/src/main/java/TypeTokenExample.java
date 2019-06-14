import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Types;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * A Key class is a type token by itself, so you can construct complex keys with long generics nicely.
 * If you dont like the excess subclassing, you can use Types.parameterized (or even Types.arrayOf),
 * as shown in getInstance call.
 */
public final class TypeTokenExample {

	public static void main(String[] args) {
		Injector injector = Injector.of(new AbstractModule() {{
			bind(String.class).toInstance("hello");
			bind(new Key<List<String>>() {}).to(s -> asList(s, s, s), String.class);
		}});

		Key<List<String>> key = Key.ofType(Types.parameterized(List.class, String.class));
		System.out.println(injector.getInstance(key));

		Key<?> complex = Key.ofType(Types.parameterized(List.class, Types.parameterized(List.class, String.class)));
		Key<List<List<String>>> subclassedButTypesafe = new Key<List<List<String>>>() {};
		System.out.println("complex == subclassedButTypesafe = " + (complex.equals(subclassedButTypesafe)));
	}
}
