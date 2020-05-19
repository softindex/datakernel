package io.datakernel.dataflow.di;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.ClassLoader.getSystemClassLoader;

public final class BinarySerializerModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(BinarySerializerModule.class);

	private final BinarySerializerLocator locator = new BinarySerializerLocator();

	private BinarySerializerModule() {
	}

	public static Module create() {
		return new BinarySerializerModule();
	}

	@Override
	protected void configure() {
		transform(0, (bindings, scope, key, binding) -> {
			if (key.getRawType() != (Class<?>) BinarySerializer.class) {
				return binding;
			}
			Class<?> rawType = key.getTypeParameter(0).getRawType();
			return binding.mapInstance(serializer -> {
				locator.serializers.putIfAbsent(rawType, (BinarySerializer<?>) serializer);
				return serializer;
			});
		});
	}

	@Provides
	BinarySerializerLocator serializerLocator() {
		return locator;
	}

	@Provides
	<T> BinarySerializer<T> serializer(Key<T> t) {
		return locator.get(t.getRawType());
	}

	public static final class BinarySerializerLocator {
		private final Map<Class<?>, BinarySerializer<?>> serializers = new HashMap<>();
		@Nullable
		private SerializerBuilder builder = null;

		@SuppressWarnings("unchecked")
		public <T> BinarySerializer<T> get(Class<T> cls) {
			return (BinarySerializer<T>) serializers.computeIfAbsent(cls, type -> {
				logger.info("Creating serializer for {}", type);
				if (builder == null) {
					builder = SerializerBuilder.create(DefiningClassLoader.create(getSystemClassLoader()));
				}
				return builder.build(type);
			});
		}
	}
}
