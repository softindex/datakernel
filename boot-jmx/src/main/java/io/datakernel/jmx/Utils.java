package io.datakernel.jmx;

import io.datakernel.jmx.api.JmxBean;
import io.datakernel.jmx.api.JmxBeanAdapter;
import org.jetbrains.annotations.NotNull;

import javax.management.DynamicMBean;
import javax.management.MXBean;
import java.lang.annotation.Annotation;
import java.util.Optional;

import static io.datakernel.common.reflection.ReflectionUtils.*;

class Utils {
	static boolean isMBean(Class<?> clazz) {
		return isJmxBean(clazz) || isStandardMBean(clazz) || isMXBean(clazz) || isDynamicMBean(clazz);
	}

	static boolean isJmxBean(Class<?> clazz) {
		return deepFindAnnotation(clazz, JmxBean.class).isPresent();
	}

	static boolean isStandardMBean(Class<?> clazz) {
		return walkClassHierarchy(clazz, anInterface ->
				anInterface.isInterface() && anInterface.getSimpleName().equals(clazz.getSimpleName() + "MBean") ?
						Optional.of(anInterface) :
						Optional.empty())
				.isPresent();
	}

	static boolean isMXBean(Class<?> clazz) {
		return walkClassHierarchy(clazz, anInterface ->
				anInterface.isInterface() && (anInterface.getSimpleName().endsWith("MXBean") || anInterface.isAnnotationPresent(MXBean.class)) ?
						Optional.of(anInterface) :
						Optional.empty())
				.isPresent();
	}

	static boolean isDynamicMBean(Class<?> clazz) {
		return DynamicMBean.class.isAssignableFrom(clazz);
	}

	static Optional<Class<? extends JmxBeanAdapter>> findAdapterClass(Class<?> aClass) {
		return deepFindAnnotation(aClass, JmxBean.class).map(JmxBean::value);
	}

	static String getQualifierString(@NotNull Object qualifier) throws ReflectiveOperationException {
		if (qualifier instanceof Class) {
			Class<?> qualifierClass = (Class<?>) qualifier;
			if (qualifierClass.isAnnotation()) {
				return qualifierClass.getSimpleName();
			}
		} else if (qualifier instanceof Annotation) {
			return getAnnotationString((Annotation) qualifier);
		}
		return qualifier.toString();
	}
}
