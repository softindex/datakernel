import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Scope;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Utils;

public class DiDependencyGraphExplore {
	static class Sugar {
		private final String name;
		private final float weight;

		@Inject
		public Sugar() {
			this.name = "Sugarella";
			this.weight = 10.f;
		}

		public Sugar(String name, float weight) {
			this.name = name;
			this.weight = weight;
		}

		public String getName() {
			return name;
		}

		public float getWeight() {
			return weight;
		}
	}

	static class Butter {
		private float weight;
		private String name;

		@Inject
		public Butter() {
			this.weight = 10.f;
			this.name = "Butter";
		}

		public Butter(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	static class Flour {
		private float weight;
		private String name;

		@Inject
		public Flour() { }

		public Flour(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	static class Pastry {
		private final Sugar sugar;
		private final Butter butter;
		private final Flour flour;

		@Inject
		Pastry(Sugar sugar, Butter butter, Flour flour) {
			this.sugar = sugar;
			this.butter = butter;
			this.flour = flour;
		}

		public Flour getFlour() {
			return flour;
		}

		public Sugar getSugar() {
			return sugar;
		}

		public Butter getButter() {
			return butter;
		}
	}

	static class Cookie {
		private final Pastry pastry;

		@Inject
		Cookie(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}

		@Override
		public String toString() {
			return "Cookie {" + pastry + '}';
		}
	}

	public static void main(String[] args) {
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(Cookie.class).to(Cookie::new, Pastry.class);
			}

			@Provides
			@OrderScope
			Sugar sugar() { return new Sugar("Sugarello", 10.f); }

			@Provides
			@OrderScope
			Butter butter() { return new Butter("Kyivmlyn", 20.0f); }

			@Provides
			@OrderScope
			Flour flour() { return new Flour("Kyivska", 100.0f); }

			@Provides
			@OrderScope
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@OrderScope
			Cookie cookie(Pastry pastry) {
				return new Cookie(pastry);
			}
		};
		Injector injector = Injector.of(cookbook);


		/// peekInstance, hasInstance and getInstance instance.
		Cookie cookie1 = injector.peekInstance(Cookie.class);
		System.out.println("Instance is present in injector before 'get' : " + injector.hasInstance(Cookie.class));
		System.out.println("Instance before get : " + cookie1);

		Cookie cookie = injector.getInstance(Cookie.class);

		Cookie cookie2 = injector.peekInstance(Cookie.class);
		System.out.println("Instance is present in injector after 'get' : " + injector.hasInstance(Cookie.class));
		System.out.println("Instance after get : " + cookie2);
		System.out.println();	/// created instance check.
		System.out.println("Instances are same : " + cookie.equals(cookie2));
		System.out.println();
		System.out.println("============================ ");
		System.out.println();


		/// parent injectors
		final Scope ORDER_SCOPE = Scope.of(OrderScope.class);

		System.out.println("Parent injector, before entering scope : " + injector.getParent());

		Injector subinjector = injector.enterScope(ORDER_SCOPE);
		System.out.println("Parent injector, after entering scope : " + subinjector.getParent());
		System.out.println("Parent injector is 'injector' : " + injector.equals(subinjector.getParent()));
		System.out.println();
		System.out.println("============================ ");
		System.out.println();

		/// bindings check
		// FYI: getBinding().toString() gives us a dependencies of current binding.
		System.out.println("Butter binding check : " + subinjector.getBinding(Pastry.class));
		System.out.println();
		System.out.println("============================ ");
		System.out.println();

		// graphviz visualization.
		Utils.printGraphVizGraph(subinjector.getBindingsTrie());
	}
}
