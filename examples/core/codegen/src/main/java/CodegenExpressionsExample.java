import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;

import java.lang.reflect.InvocationTargetException;

import static io.datakernel.codegen.Expressions.*;
import static java.lang.ClassLoader.getSystemClassLoader;

//[START EXAMPLE]
public final class CodegenExpressionsExample {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		Class<Example> example = ClassBuilder
				// context class loader is used because without it maven runner is not happy with our codegen
				.create(DefiningClassLoader.create(getSystemClassLoader()), Example.class)
				.withMethod("sayHello", call(staticField(System.class, "out"), "println", value("Hello world")))
				.build();
		Example instance = example.getConstructor().newInstance();

		instance.sayHello();
	}

	public interface Example {
		void sayHello();
	}
}
//[END EXAMPLE]
