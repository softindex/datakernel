import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.binding.Binding;
import io.datakernel.di.module.AbstractModule;

import java.util.Optional;

public class BindingGeneratorExample {
	static class Sugar {
		private final String name;
		private final float weight;

		@Inject
		public Sugar() {
			this.name = "WhiteSugar";
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
		private final float weight;
		private final String name;

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

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	//[START REGION_1]
	static class Cookie<T> {
		private final Optional<T> pastry;

		@Inject
		Cookie(Optional<T> pastry) {
			this.pastry = pastry;
		}

		public Optional<T> getPastry() {
			return pastry;
		}
	}
	//[END REGION_1]

	public static void main(String[] args) {
		//[START REGION_2]
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				// note (1)
				generate(Optional.class, (bindings, scope, key) -> {
					Binding<Object> binding = bindings.get(key.getTypeParameter(0));
					return binding != null ?
							binding.mapInstance(Optional::of) :
							Binding.toInstance(Optional.empty());
				});

				bind(new Key<Cookie<Pastry>>() {});
			}
			//[END REGION_2]

			@Provides
			Sugar sugar() { return new Sugar("WhiteSugar", 10.f); }

			@Provides
			Flour flour() { return new Flour("PerfectButter", 20.0f); }

			@Provides
			Butter butter() { return new Butter("PerfectButter", 10.0f); }

			@Provides
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			// note (2) : same as generate() in this.configure() ...
//			@Provides
//			<T> Optional<T> pastry(@io.datakernel.di.annotation.Optional T instance) {
//				return Optional.ofNullable(instance);
//			}
		};

		//[START REGION_3]
		Injector injector = Injector.of(cookbook);
		System.out.println(injector.getInstance(new Key<Cookie<Pastry>>() {}).getPastry().get().getButter().getName());
		//[END REGION_3]
	}

}
