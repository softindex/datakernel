package io.datakernel.jmx;

import io.datakernel.common.ref.RefBoolean;
import io.datakernel.common.reflection.ReflectionUtils;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxRefreshHandler;
import io.datakernel.jmx.api.JmxWrapperFactory;
import io.datakernel.jmx.api.MBeanWrapperFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.DynamicMBean;
import javax.management.MXBean;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.datakernel.common.reflection.ReflectionUtils.*;
import static java.util.stream.Collectors.toSet;

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
		return deepFindAnnotation(clazz, annotation -> annotation.annotationType() == MBeanWrapperFactory.class) != null;
	}

	static boolean isDynamicMBean(Class<?> clazz) {
		return DynamicMBean.class.isAssignableFrom(clazz);
	}

	static boolean isMBean(Class<?> clazz) {
		return isJmxMBean(clazz) || isStandardMBean(clazz) || isMXBean(clazz) || isDynamicMBean(clazz);
	}

	private static boolean isBeanInterface(Class<?> cls) {
		String name = cls.getName();
		return name.endsWith("MBean") || name.endsWith("MXBean") || cls.isAnnotationPresent(MXBean.class);
	}

	public static boolean isBean(Class<?> cls) {
		return getAllInterfaces(cls).stream().anyMatch(Utils::isBeanInterface);
	}

	@Nullable
	static Class<? extends JmxWrapperFactory> getWrapperFactoryClass(Class<?> clazz) {
		Annotation wrapperFactoryAnnotation = deepFindAnnotation(clazz, annotation -> annotation.annotationType() == MBeanWrapperFactory.class);
		if (wrapperFactoryAnnotation == null) return null;
		return ((MBeanWrapperFactory) wrapperFactoryAnnotation).value();
	}

	static Collection<Integer> extractRefreshStats(Collection<JmxWrapperFactory> wrappers, Function<JmxRefreshHandler, Collection<Integer>> statsExtractor) {
		return wrappers.stream()
				.filter(jmxWrapperFactory -> jmxWrapperFactory instanceof JmxRefreshHandler)
				.flatMap(jmxWrapperFactory -> statsExtractor.apply((JmxRefreshHandler) jmxWrapperFactory).stream())
				.collect(Collectors.toList());
	}

	public static Map<String, Object> getJmxAttributes(@Nullable Object instance) {
		Map<String, Object> result = new LinkedHashMap<>();
		getJmxAttributes(instance, "", result);
		return result;
	}

	private static boolean getJmxAttributes(@Nullable Object instance, String rootName, Map<String, Object> attrs) {
		if (instance == null) {
			return false;
		}
		Set<String> attributeNames = getAllInterfaces(instance.getClass()).stream()
				.filter(Utils::isBeanInterface)
				.flatMap(i -> Arrays.stream(i.getMethods())
						.filter(ReflectionUtils::isGetter)
						.map(Method::getName))
				.collect(toSet());
		RefBoolean changed = new RefBoolean(false);
		Arrays.stream(instance.getClass().getMethods())
				.filter(method -> isGetter(method) && (attributeNames.contains(method.getName())
						|| Arrays.stream(method.getAnnotations()).anyMatch(a -> a.annotationType() == JmxAttribute.class)))
				.sorted(Comparator.comparing(Method::getName))
				.forEach(method -> {
					Object fieldValue;
					method.setAccessible(true); // anonymous classes do not work without this, wow
					try {
						fieldValue = method.invoke(instance);
					} catch (IllegalAccessException | InvocationTargetException e) {
						return;
					}
					changed.set(true);
					JmxAttribute annotation = method.getAnnotation(JmxAttribute.class);
					String name = annotation == null || annotation.name().equals(JmxAttribute.USE_GETTER_NAME) ?
							extractFieldNameFromGetter(method) :
							annotation.name();
					name = rootName.isEmpty() ? name : rootName + (name.isEmpty() ? "" : "_" + name);
					if (fieldValue != instance && !attrs.containsKey(name) && !getJmxAttributes(fieldValue, name, attrs)) {
						attrs.put(name, fieldValue);
					}
				});
		return changed.get();
	}

	static String getQualifierString(@NotNull Object qualifier) throws ReflectiveOperationException {
		if (qualifier instanceof Class) {
			if (((Class<?>) qualifier).isAnnotation()) {
				//noinspection unchecked
				return getAnnotationString((Class<? extends Annotation>) qualifier, null);
			}
		} else if (qualifier instanceof Annotation) {
			return getAnnotationString((Annotation) qualifier);
		}
		return qualifier.toString();
	}
}
