package io.datakernel.example.codegen;

import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;

import static io.datakernel.codegen.Expressions.*;
import static java.lang.ClassLoader.getSystemClassLoader;

public final class CodegenExpressionsExample {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		//[START REGION_1]
		Class<Example> example = ClassBuilder
				// DefiningClassLoader represents a loader for defining dynamically generated classes
				.create(DefiningClassLoader.create(getSystemClassLoader()), Example.class)
				.withMethod("sayHello", call(staticField(System.class, "out"), "println", value("Hello world")))
				.build();
		//[END REGION_1]

		//[START REGION_2]
		Example instance = example.newInstance();
		instance.sayHello();
		//[END REGION_2]
	}

	//[START REGION_3]
	public interface Example {
		void sayHello();
	}
	//[END REGION_3]
}
