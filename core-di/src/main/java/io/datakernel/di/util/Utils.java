package io.datakernel.di.util;

import io.datakernel.di.*;
import io.datakernel.di.module.Provides;
import io.datakernel.util.TypeT;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;

public class Utils {
	@NotNull
	public static <K, V> Map<K, V> flattenMultimap(Map<K, Set<V>> multimap, Function<K, Function<Set<V>, V>> reducers) {
		return multimap.entrySet().stream()
				.collect(toMap(
						Map.Entry::getKey,
						entry -> {
							Set<V> value = entry.getValue();
							switch (value.size()) {
								case 0:
									throw new IllegalStateException();
								case 1:
									return value.iterator().next();
								default:
									return reducers.apply(entry.getKey()).apply(entry.getValue());
							}
						})
				);
	}

	public static <K, V> void combineMultimap(Map<K, Set<V>> accumulator, Map<K, Set<V>> multimap) {
		multimap.forEach((key, set) -> accumulator.computeIfAbsent(key, $ -> new HashSet<>()).addAll(set));
	}

	@NotNull
	public static Key<Object> keyOf(@NotNull Type type, Annotation[] annotations) {
		Set<Annotation> names = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(NameAnnotation.class) != null)
				.collect(toSet());
		if (names.size() > 1) {
			throw new RuntimeException("more than one name annotation");
		}
		return names.isEmpty() ?
				Key.of(TypeT.ofType(type)) :
				Key.of(TypeT.ofType(type), names.iterator().next());
	}

	public static Key<Object> keyOf(Field field, TypeVariable<?>[] typeParameters, Type[] actualTypeArguments) {
		Type fieldType = field.getGenericType();

		for (int i = 0; i < typeParameters.length; i++) {
			if (fieldType == typeParameters[i]) {
				fieldType = actualTypeArguments[i];
				break;
			}
		}
		return keyOf(fieldType, field.getDeclaredAnnotations());
	}

	public static Set<Binding<?>> bindingsFromProvidesAnnotations(Object instance) {
		Class<?> cls = instance.getClass();
		return Arrays.stream(cls.getDeclaredMethods())
				.filter(method -> method.getAnnotation(Provides.class) != null)
				.map(method -> {

					Field[] fieldsToInject = takeWhile(Stream.<Class<?>>iterate(method.getReturnType(), Class::getSuperclass), Objects::nonNull)
							.flatMap(c -> Arrays.stream(c.getDeclaredFields()))
							.filter(field -> field.getAnnotation(Inject.class) != null)
							.peek(field -> field.setAccessible(true))
							.toArray(Field[]::new);

					TypeVariable<?>[] typeParameters = method.getReturnType().getTypeParameters();
					Type[] actualTypeArguments;
					if (method.getGenericReturnType() instanceof ParameterizedType) {
						actualTypeArguments = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments();
					} else if (typeParameters.length > 0) {
						throw new RuntimeException("generic return type");
					} else {
						actualTypeArguments = new Type[0];
					}

					Parameter[] parameters = method.getParameters();
					int parameterCount = parameters.length;

					Dependency[] dependencies = new Dependency[parameterCount + fieldsToInject.length];
					for (int i = 0; i < parameterCount; i++) {
						dependencies[i] = new Dependency(keyOf(parameters[i].getParameterizedType(), parameters[i].getDeclaredAnnotations()), true);
					}
					for (int i = 0; i < fieldsToInject.length; i++) {
						boolean required = !fieldsToInject[i].getAnnotation(Inject.class).optional();
						dependencies[parameterCount + i] = new Dependency(keyOf(fieldsToInject[i], typeParameters, actualTypeArguments), required);
					}

					Key<Object> key = keyOf(method.getGenericReturnType(), method.getDeclaredAnnotations());

					method.setAccessible(true);

					return new Binding<>(key, dependencies, args -> {
						try {
							Object provided = method.invoke(instance, Arrays.copyOfRange(args, 0, parameterCount));
							for (int i = 0; i < fieldsToInject.length; i++) {
								Object arg = args[parameterCount + i];
								if (arg != null) {
									fieldsToInject[i].set(provided, arg);
								}
							}
							return provided;
						} catch (IllegalAccessException | InvocationTargetException e) {
							throw new RuntimeException("failed to call method", e);
						}
					});
				})
				.collect(toSet());
	}

	private static <T> Spliterator<T> takeWhile(Spliterator<T> spliterator, Predicate<? super T> predicate) {
		return new Spliterators.AbstractSpliterator<T>(spliterator.estimateSize(), 0) {
			boolean stillGoing = true;

			@Override
			public boolean tryAdvance(Consumer<? super T> consumer) {
				if (!stillGoing) {
					return false;
				}
				boolean hadNext = spliterator.tryAdvance(elem -> {
					if (predicate.test(elem)) {
						consumer.accept(elem);
					} else {
						stillGoing = false;
					}
				});
				return hadNext && stillGoing;
			}
		};
	}

	// because WHY ON EARTH they did not add so many common basic stream operations to their streams
	// todo when using java >= 9 - replace it with java 9 version
	public static <T> Stream<T> takeWhile(Stream<T> stream, Predicate<? super T> predicate) {
		return StreamSupport.stream(takeWhile(stream.spliterator(), predicate), false);
	}
}
