package io.datakernel.dataflow.di;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.tuple.*;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.*;

import static io.datakernel.codec.StructuredCodecs.*;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class CodecsModule extends AbstractModule {

	private CodecsModule() {
	}

	public static Module create() {
		return new CodecsModule();
	}

	@NameAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Subtypes {
	}

	@FunctionalInterface
	public interface SubtypeNameFactory {
		@Nullable
		String getName(Class<?> subtype);
	}

	@Override
	protected void configure() {
		bindPrimitive(void.class, VOID_CODEC);
		bindPrimitive(boolean.class, BOOLEAN_CODEC);
		bindPrimitive(char.class, CHARACTER_CODEC);
		bindPrimitive(byte.class, BYTE_CODEC);
		bindPrimitive(int.class, INT_CODEC);
		bindPrimitive(long.class, LONG_CODEC);
		bindPrimitive(float.class, FLOAT_CODEC);
		bindPrimitive(double.class, DOUBLE_CODEC);

		bindPrimitive(Void.class, VOID_CODEC);
		bindPrimitive(Boolean.class, BOOLEAN_CODEC);
		bindPrimitive(Character.class, CHARACTER_CODEC);
		bindPrimitive(Byte.class, BYTE_CODEC);
		bindPrimitive(Integer.class, INT_CODEC);
		bindPrimitive(Long.class, LONG_CODEC);
		bindPrimitive(Float.class, FLOAT_CODEC);
		bindPrimitive(Double.class, DOUBLE_CODEC);

		bindPrimitive(String.class, STRING_CODEC);

		bindPrimitive(byte[].class, BYTES_CODEC);

		bind(new Key<StructuredCodec<Class<?>>>() {}).toInstance(CLASS_CODEC);

		generate(StructuredCodec.class, (bindings, scope, key) -> {
			if (key.getAnnotationType() != Subtypes.class) {
				return null;
			}
			Class<?> type = key.getTypeParameter(0).getRawType();
			return Binding.to(args -> {
				Injector injector = (Injector) args[0];
				SubtypeNameFactory names = (SubtypeNameFactory) args[1];
				if (names == null) {
					names = $ -> null;
				}

				Set<Class<?>> subtypes = new HashSet<>();

				Injector i = injector;
				while (i != null) {
					for (Key<?> k : i.getBindings().keySet()) {
						if (k.getRawType() != StructuredCodec.class) {
							continue;
						}
						Class<?> subtype = k.getTypeParameter(0).getRawType();
						if (type != subtype && type.isAssignableFrom(subtype)) {
							subtypes.add(subtype);
						}
					}
					i = i.getParent();
				}

				CodecSubtype<Object> combined = CodecSubtype.create();
				for (Class<?> subtype : subtypes) {
					StructuredCodec<?> codec = injector.getInstance(Key.ofType(Types.parameterized(StructuredCodec.class, subtype)));
					String name = names.getName(subtype);
					if (name != null) {
						combined.with(subtype, name, codec);
					} else {
						combined.with(subtype, codec);
					}
				}
				return combined;
			}, new Dependency[]{Dependency.toKey(Key.of(Injector.class)), Dependency.toOptionalKey(Key.of(SubtypeNameFactory.class))});
		});
	}

	private <T> void bindPrimitive(Class<T> cls, StructuredCodec<T> codec) {
		bind(Key.ofType(Types.parameterized(StructuredCodec.class, cls))).toInstance(codec);
	}

	@Provides
	<T> StructuredCodec<Optional<T>> optional(StructuredCodec<T> itemCodec) {
		return ofOptional(itemCodec);
	}

	@Provides
	<T> StructuredCodec<Set<T>> set(StructuredCodec<T> itemCodec) {
		return ofSet(itemCodec);
	}

	@Provides
	<T> StructuredCodec<List<T>> list(StructuredCodec<T> itemCodec) {
		return ofList(itemCodec);
	}

	@Provides
	<K, V> StructuredCodec<Map<K, V>> map(StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return ofMap(keyCodec, valueCodec);
	}

	@Provides
	<T1> StructuredCodec<Tuple1<T1>> tuple1(StructuredCodec<T1> codec1) {
		return tuple(Tuple1::new,
				Tuple1::getValue1, codec1);
	}

	@Provides
	<T1, T2> StructuredCodec<Tuple2<T1, T2>> tuple2(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2) {
		return tuple(Tuple2::new,
				Tuple2::getValue1, codec1,
				Tuple2::getValue2, codec2);
	}

	@Provides
	<T1, T2, T3> StructuredCodec<Tuple3<T1, T2, T3>> tuple3(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3) {
		return tuple(Tuple3::new,
				Tuple3::getValue1, codec1,
				Tuple3::getValue2, codec2,
				Tuple3::getValue3, codec3);
	}

	@Provides
	<T1, T2, T3, T4> StructuredCodec<Tuple4<T1, T2, T3, T4>> tuple4(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4) {
		return tuple(Tuple4::new,
				Tuple4::getValue1, codec1,
				Tuple4::getValue2, codec2,
				Tuple4::getValue3, codec3,
				Tuple4::getValue4, codec4);
	}

	@Provides
	<T1, T2, T3, T4, T5> StructuredCodec<Tuple5<T1, T2, T3, T4, T5>> tuple5(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4, StructuredCodec<T5> codec5) {
		return tuple(Tuple5::new,
				Tuple5::getValue1, codec1,
				Tuple5::getValue2, codec2,
				Tuple5::getValue3, codec3,
				Tuple5::getValue4, codec4,
				Tuple5::getValue5, codec5);
	}

	@Provides
	<T1, T2, T3, T4, T5, T6> StructuredCodec<Tuple6<T1, T2, T3, T4, T5, T6>> tuple6(StructuredCodec<T1> codec1, StructuredCodec<T2> codec2, StructuredCodec<T3> codec3, StructuredCodec<T4> codec4, StructuredCodec<T5> codec5, StructuredCodec<T6> codec6) {
		return tuple(Tuple6::new,
				Tuple6::getValue1, codec1,
				Tuple6::getValue2, codec2,
				Tuple6::getValue3, codec3,
				Tuple6::getValue4, codec4,
				Tuple6::getValue5, codec5,
				Tuple6::getValue6, codec6);
	}
}
