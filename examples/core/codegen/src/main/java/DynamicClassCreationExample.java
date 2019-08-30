import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.ExpressionToString;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.codegen.Expressions.*;

/**
 * In this example a Class that implements the specified interface is dynamically created.
 * Methods are constructed programmatically using our fluent API built on top of ASM.
 */
public class DynamicClassCreationExample {
	//[START REGION_1]
	public static void main(String[] args) throws IllegalAccessException, InstantiationException {
		// Construct a Class that implements Person interface
		Class<Person> personClass = ClassBuilder.create(DefiningClassLoader.create(Thread.currentThread().getContextClassLoader()), Person.class)
				// declare fields
				.withField("id", int.class)
				.withField("name", String.class)
				// setter for both fields - a sequence of actions
				.withMethod("setIdAndName", sequence(
						set(property(self(), "id"), arg(0)),
						set(property(self(), "name"), arg(1))))
				.withMethod("getId", property(self(), "id"))
				.withMethod("getName", property(self(), "name"))
				// compareTo, equals, hashCode and toString methods implementations follow the standard convention
				.withMethod("int compareTo(Person)", compareTo("id", "name"))
				.withMethod("equals", asEquals("id", "name"))
				.withMethod("hashOfPojo", hashCodeOfArgs(property(arg(0), "id"), property(arg(0), "name")))
				.withMethod("hash", hashCodeOfArgs(property(self(), "id"), property(self(), "name")))
				.withMethod("toString", ((ExpressionToString) asString())
						.withQuotes("{", "}", ", ")
						.withArgument("id: ", property(self(), "id"))
						.withArgument("name: ", property(self(), "name")))
				.build();

		// Instantiate two objects of dynamically defined class
		Person jack = personClass.newInstance();
		Person martha = personClass.newInstance();

		jack.setIdAndName(5, "Jack");
		martha.setIdAndName(jack.getId() * 2, "Martha");

		System.out.println("First person: " + jack);
		System.out.println("Second person: " + martha);

		System.out.println("jack.equals(martha) ? : " + jack.equals(martha));

		// Compare dynamically created hashing implementation with the conventional one
		ExamplePojo examplePojo = new ExamplePojo(5, "Jack");
		System.out.println(examplePojo);
		System.out.println("jack.hash(examplePojo)  = " + jack.hashOfPojo(examplePojo));
		System.out.println("jack.hash()             = " + jack.hash());
		System.out.println("examplePojo.hashCode()  = " + examplePojo.hashCode());
	}
	//[END REGION_1]

	public interface Person extends Comparable<Person> {
		void setIdAndName(int id, String name);

		int getId();

		String getName();

		int hashOfPojo(ExamplePojo personPojo);

		int hash();

		@Override
		int compareTo(@NotNull Person o);

		@Override
		String toString();

		@Override
		boolean equals(Object obj);
	}

	public static class ExamplePojo {
		int id;
		String name;

		ExamplePojo(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public int hashCode() {
			int result = id;
			result = 31 * result + name.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "PersonPojo{id=" + id + ", name=" + name + '}';
		}
	}

}
