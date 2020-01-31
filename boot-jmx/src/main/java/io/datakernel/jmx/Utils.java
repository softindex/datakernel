package io.datakernel.jmx;

import io.datakernel.eventloop.jmx.EventloopJmxMBean;
import io.datakernel.jmx.api.ConcurrentJmxMBean;

import javax.management.DynamicMBean;
import javax.management.MXBean;
import java.util.List;

@SuppressWarnings("WeakerAccess")
class Utils {

	static boolean allInstancesAreOfSameType(List<?> instances) {
		Class<?> firstClass = instances.get(0).getClass();
		for (int i = 1; i < instances.size(); i++) {
			if (!firstClass.equals(instances.get(i).getClass())) {
				return false;
			}
		}
		return true;
	}

	static boolean isStandardMBean(Class<?> clazz) {
		return classImplementsInterfaceWithNameEndingWith(clazz, "MBean");
	}

	static boolean isMXBean(Class<?> clazz) {
		return classFollowsMXBeanConvention(clazz);
	}

	static boolean classImplementsInterfaceWithNameEndingWith(Class<?> clazz, String ending) {
		String clazzName = clazz.getSimpleName();
		Class<?>[] interfaces = clazz.getInterfaces();
		for (Class<?> anInterface : interfaces) {
			String interfaceName = anInterface.getSimpleName();
			if (interfaceName.equals(clazzName + ending)) {
				return true;
			}
		}
		return false;
	}

	static boolean classFollowsMXBeanConvention(Class<?> clazz) {
		Class<?>[] interfazes = clazz.getInterfaces();
		for (Class<?> interfaze : interfazes) {
			if (interfaceFollowsMXBeanConvention(interfaze)) {
				return true;
			}
		}

		Class<?> superClazz = clazz.getSuperclass();
		if (superClazz != null) {
			return classFollowsMXBeanConvention(superClazz);
		}

		return false;
	}

	static boolean interfaceFollowsMXBeanConvention(Class<?> interfaze) {
		if (interfaze.getSimpleName().endsWith("MXBean") || interfaze.isAnnotationPresent(MXBean.class)) {
			return true;
		}

		Class<?>[] subInterfazes = interfaze.getInterfaces();
		for (Class<?> subInterfaze : subInterfazes) {
			if (interfaceFollowsMXBeanConvention(subInterfaze)) {
				return true;
			}
		}

		return false;
	}

	static boolean isJmxMBean(Class<?> clazz) {
		return ConcurrentJmxMBean.class.isAssignableFrom(clazz) || EventloopJmxMBean.class.isAssignableFrom(clazz);
	}

	static boolean isDynamicMBean(Class<?> clazz) {
		return DynamicMBean.class.isAssignableFrom(clazz);
	}

	static boolean isMBean(Class<?> clazz) {
		return isJmxMBean(clazz) || isStandardMBean(clazz) || isMXBean(clazz) || isDynamicMBean(clazz);
	}
}
