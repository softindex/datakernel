package io.datakernel.di.core;

import io.datakernel.di.module.Module;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class TestInjectorIternals {

	interface SomeService {}

	class SomeServiceImpl implements SomeService {}

	class Container {
		private final SomeService service;

		public Container(SomeService service) {
			this.service = service;
		}

		public SomeService getService() {
			return service;
		}
	}

	@Test
	public void plainBindIndexes() {
		Injector injector = Injector.of(Module.create()
				.bind(SomeServiceImpl.class).to(SomeServiceImpl::new)
				.bind(SomeService.class).to(SomeServiceImpl.class));

		assertSame(injector.getInstanceOrNull(SomeService.class), injector.getInstanceOrNull(SomeServiceImpl.class));
		assertEquals(2, injector.scopeCaches[0].length()); // always the injector itself + only one instance
		assertEquals(Stream.of(0, 1, 1).collect(toSet()), new HashSet<>(injector.localSlotMapping.values())); // the injector + 2 entries pointing to same index
	}

	@Test
	public void mappedPlainBind() {
		Injector injector = Injector.of(Module.create()
				.bind(SomeServiceImpl.class).to(SomeServiceImpl::new)
				.bind(SomeService.class).to(SomeServiceImpl.class)
				.bind(Container.class).to(Binding.to(SomeServiceImpl.class).mapInstance(Container::new)));

		assertSame(injector.getInstanceOrNull(SomeService.class), injector.getInstanceOrNull(SomeServiceImpl.class));

		assertSame(injector.getInstanceOrNull(Container.class), injector.getInstanceOrNull(Container.class));
		assertSame(injector.getInstanceOrNull(SomeService.class), injector.getInstanceOrNull(SomeService.class));
		assertSame(injector.getInstanceOrNull(SomeService.class), injector.getInstanceOrNull(SomeServiceImpl.class));
		assertSame(injector.getInstanceOrNull(SomeServiceImpl.class), injector.getInstanceOrNull(SomeService.class));

		assertEquals(3, injector.scopeCaches[0].length()); // the injector + impl instance + string

		assertEquals(Stream.of(0, 1, 1, 2).collect(toSet()), new HashSet<>(injector.localSlotMapping.values())); // the injector + 2 entries pointing to same index + 1 entry for string
	}

	@Test
	public void mapInstanceSameIndex() {
		AtomicInteger counter1 = new AtomicInteger();
		AtomicInteger counter2 = new AtomicInteger();

		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).to(() -> {
					counter1.incrementAndGet();
					return 123;
				})
				.bind(String.class).to(Binding.to(Integer.class).mapInstance(i -> {
					counter2.incrementAndGet();
					return "str_" + i;
				})));

		injector.getInstance(String.class);
		injector.getInstance(String.class);
		injector.getInstance(String.class);
		injector.getInstance(String.class);

		assertEquals(1, counter1.get());
		assertEquals(1, counter2.get());
	}
}
