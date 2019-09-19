package io.datakernel.di;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.util.Trie;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.*;

/**
 * To represent the main concepts and features of DataKernel DI,
 * we've created an example which starts with low-level DI
 * concepts and gradually covers more specific advanced features.
 * <p>
 * In this example we have a kitchen, where you can
 * automatically create tasty cookies in different ways
 * with our wonderful DI.
 */
public class DIFollowUpTest {
	//[START REGION_9]
	public static final Scope ORDER_SCOPE = Scope.of(OrderScope.class);
	//[END REGION_9]

	static class Kitchen {
		private final int places;

		@Inject
		Kitchen() {
			this.places = 1;
		}

		public int getPlaces() {
			return places;
		}
	}

	//[START REGION_8]
	static class Sugar {
		private final String name;
		private final float weight;

		@Inject
		public Sugar() {
			this.name = "WhiteSugar";
			this.weight = 10.f;
		}
		//[END REGION_8]

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
	}

	@Test
	//[START REGION_1]
	public void manualBindSnippet() {
		Map<Key<?>, Binding<?>> bindings = new LinkedHashMap<>();
		bindings.put(Key.of(Sugar.class), Binding.to(() -> new Sugar("WhiteSugar", 10.0f)));
		bindings.put(Key.of(Butter.class), Binding.to(() -> new Butter("PerfectButter", 20.0f)));
		bindings.put(Key.of(Flour.class), Binding.to(() -> new Flour("GoodFlour", 100.0f)));
		bindings.put(Key.of(Pastry.class), Binding.to(Pastry::new, Sugar.class, Butter.class, Flour.class));
		bindings.put(Key.of(Cookie.class), Binding.to(Cookie::new, Pastry.class));

		Injector injector = Injector.of(Trie.leaf(bindings));
		Cookie instance = injector.getInstance(Cookie.class);

		assertEquals(10.f, instance.getPastry().getSugar().getWeight());
	}
	//[END REGION_1]

	@Test
	//[START REGION_2]
	public void moduleBindSnippet() {
		Module module = Module.create()
				.bind(Sugar.class).to(() -> new Sugar("WhiteSugar", 10.0f))
				.bind(Butter.class).to(() -> new Butter("PerfectButter", 20.0f))
				.bind(Flour.class).to(() -> new Flour("GoodFlour", 100.0f))
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class);

		Injector injector = Injector.of(module);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).getPastry().getButter().getName());
	}
	//[END REGION_2]

	@Test
	//[START REGION_3]
	public void provideAnnotationSnippet() {
		Module cookbook = new AbstractModule() {
			@Provides
			Sugar sugar() { return new Sugar("WhiteSugar", 10.f); }

			@Provides
			Butter butter() { return new Butter("PerfectButter", 20.0f); }

			@Provides
			Flour flour() { return new Flour("GoodFlour", 100.0f); }

			@Provides
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			Cookie cookie(Pastry pastry) {
				return new Cookie(pastry);
			}
		};

		Injector injector = Injector.of(cookbook);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).getPastry().getButter().getName());
	}
	//[END REGION_3]

	@Test
	//[START REGION_4]
	public void injectAnnotationSnippet() {
		Module cookbook = Module.create().bind(Cookie.class);

		Injector injector = Injector.of(cookbook);
		assertEquals("WhiteSugar", injector.getInstance(Cookie.class).getPastry().getSugar().getName());
	}
	//[END REGION_4]

	@Test
	//[START REGION_5]
	public void namedAnnotationSnippet() {
		Module cookbook = new AbstractModule() {
			@Provides
			@Named("zerosugar")
			Sugar sugar1() { return new Sugar("SugarFree", 0.f); }

			@Provides
			@Named("normal")
			Sugar sugar2() { return new Sugar("WhiteSugar", 10.f); }

			@Provides
			Butter butter() { return new Butter("PerfectButter", 20.f); }

			@Provides
			Flour flour() { return new Flour("GoodFlour", 100.f); }

			@Provides
			@Named("normal")
			Pastry pastry1(@Named("normal") Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@Named("zerosugar")
			Pastry pastry2(@Named("zerosugar") Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@Named("normal")
			Cookie cookie1(@Named("normal") Pastry pastry) {
				return new Cookie(pastry);
			}

			@Provides
			@Named("zerosugar")
			Cookie cookie2(@Named("zerosugar") Pastry pastry) { return new Cookie(pastry); }
		};

		Injector injector = Injector.of(cookbook);

		float normalWeight = injector.getInstance(Key.of(Cookie.class, "normal"))
				.getPastry().getSugar().getWeight();
		float zerosugarWeight = injector.getInstance(Key.of(Cookie.class, "zerosugar"))
				.getPastry().getSugar().getWeight();

		assertEquals(10.f, normalWeight);
		assertEquals(0.f, zerosugarWeight);
	}
	//[END REGION_5]

	@Test
	public void orderAnnotationSnippet() {
		//[START REGION_10]
		Module cookbook = Module.create()
				.bind(Kitchen.class).to(Kitchen::new)
				.bind(Sugar.class).to(Sugar::new).in(OrderScope.class)
				.bind(Butter.class).to(Butter::new).in(OrderScope.class)
				.bind(Flour.class).to(Flour::new).in(OrderScope.class)
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class).in(OrderScope.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class).in(OrderScope.class);
		//[END REGION_10]

		//[START REGION_6]
		Injector injector = Injector.of(cookbook);
		Kitchen kitchen = injector.getInstance(Kitchen.class);
		List<Cookie> cookies = new ArrayList<>();
		for (int i = 0; i < 10; ++i) {
			Injector subinjector = injector.enterScope(ORDER_SCOPE);

			assertSame(subinjector.getInstance(Kitchen.class), kitchen);
			if (i > 0) assertNotSame(cookies.get(i - 1), subinjector.getInstance(Cookie.class));

			cookies.add(subinjector.getInstance(Cookie.class));
		}
		assertEquals(10, cookies.size());
		//[END REGION_6]
	}

	@Test
	//[START REGION_7]
	public void transformBindingSnippet() {
		Module cookbook = Module.create()
				.bind(Sugar.class).to(Sugar::new)
				.bind(Butter.class).to(Butter::new)
				.bind(Flour.class).to(() -> new Flour("GoodFlour", 100.0f))
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class)
				.transform(0, (bindings, scope, key, binding) ->
						binding.onInstance(x -> System.out.println(Instant.now() + " -> " + key)));

		Injector injector = Injector.of(cookbook);
		assertEquals("GoodFlour", injector.getInstance(Cookie.class).getPastry().getFlour().getName());
	}
	//[END REGION_7]
}
