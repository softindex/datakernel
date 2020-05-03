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

public final class BinarySerializersModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(BinarySerializersModule.class);

	private final BinarySerializers serializers = new BinarySerializers();

	private BinarySerializersModule() {
	}

	public static Module create() {
		return new BinarySerializersModule();
	}

	@Override
	protected void configure() {
		transform(0, (bindings, scope, key, binding) -> {
			if (key.getRawType() != (Class<?>) BinarySerializer.class) {
				return binding;
			}
			Class<?> rawType = key.getTypeParameter(0).getRawType();
			return binding.mapInstance(serializer -> {
				serializers.serializers.putIfAbsent(rawType, (BinarySerializer<?>) serializer);
				return serializer;
			});
		});
	}

	@Provides
	BinarySerializers serializers() {
		return serializers;
	}

	@Provides
	<T> BinarySerializer<T> serializer(Key<T> t) {
		return serializers.get(t.getRawType());
	}

	public static final class BinarySerializers {
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
