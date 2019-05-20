package io.datakernel.jmx;

import io.datakernel.di.Injector;
import io.datakernel.jmx.GlobalSingletonsRegistrationTest.GlobalSingletonClass1.CustomClass;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GlobalSingletonsRegistrationTest {
	private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

	@Test
	public void testMXBeanSingletonRegistration() throws Exception {
//		int jmxValue = 55;
//		GlobalSingletonClass2 globalSingletonClass2 = GlobalSingletonClass2.getInstance();
//		globalSingletonClass2.setValue(jmxValue);

//		Injector injector = Guice.createInjector(
//				ServiceGraphModule.defaultInstance(),
//				JmxModule.create()
//						.withGlobalSingletons(globalSingletonClass2)
//		);
//
//		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
//		ObjectName found;
//		try {
//			serviceGraph.startFuture().get();
//			ObjectName objectName = server.queryNames(new ObjectName("*:type=" + GlobalSingletonClass2.class.getSimpleName() + ",*"), null).iterator().next();
//			assertEquals(jmxValue, server.getAttribute(objectName, "Value"));
//			found = objectName;
//		} finally {
//			serviceGraph.stopFuture().get();
//			unregisterMBeans();
//		}
//		assertNotNull(found);
	}

	@Test
	public void testMBeanSingletonRegistration() throws Exception {
//		GlobalSingletonClass1 globalSingletonClass1 = GlobalSingletonClass1.getInstance();
//
//		Injector injector = Guice.createInjector(
//				ServiceGraphModule.defaultInstance(),
//				JmxModule.create()
//						.withGlobalSingletons(globalSingletonClass1)
//		);
//
//		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
//		ObjectName found;
//		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
//		try {
//			serviceGraph.startFuture().get();
//			ObjectName objectName = server.queryNames(new ObjectName("*:type=" + GlobalSingletonClass1.class.getSimpleName() + ",*"), null).iterator().next();
//			assertEquals(globalSingletonClass1.custom, server.getAttribute(objectName, "CustomClass"));
//			found = objectName;
//		} finally {
//			serviceGraph.stopFuture().get();
//			unregisterMBeans();
//		}
//		assertNotNull(found);
	}

	private void unregisterMBeans() throws Exception {
		server.unregisterMBean(new ObjectName("io.datakernel.bytebuf:type=ByteBufPoolStats"));
		server.unregisterMBean(new ObjectName("io.datakernel.service:type=ServiceGraph"));
		server.unregisterMBean(new ObjectName("io.datakernel.jmx:type=JmxRegistry"));
	}

	// region singletons
	public interface GlobalSingletonClass1MBean {
		CustomClass getCustomClass();
	}

	static class GlobalSingletonClass1 implements GlobalSingletonClass1MBean {
		private static final GlobalSingletonClass1 INSTANCE = new GlobalSingletonClass1();
		private CustomClass custom = new CustomClass();

		private GlobalSingletonClass1() {}

		public static GlobalSingletonClass1 getInstance() {
			return INSTANCE;
		}

		@Override
		public CustomClass getCustomClass() {
			return custom;
		}

		class CustomClass {}
	}

	public interface StubMXBean {
		int getValue();
	}

	public static class GlobalSingletonClass2 implements StubMXBean {
		private static final GlobalSingletonClass2 INSTANCE = new GlobalSingletonClass2();
		private int value;

		private GlobalSingletonClass2() {}

		public static GlobalSingletonClass2 getInstance() {
			return INSTANCE;
		}

		@Override
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}
	}
	// endregion
}
