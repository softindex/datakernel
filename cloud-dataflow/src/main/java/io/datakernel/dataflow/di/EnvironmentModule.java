package io.datakernel.dataflow.di;

import io.datakernel.di.Key;
import io.datakernel.di.annotation.QualifierAnnotation;
import io.datakernel.di.binding.Binding;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class EnvironmentModule extends AbstractModule {
	private EnvironmentModule() {
	}

	public static Module create() {
		return new EnvironmentModule();
	}

	@Override
	protected void configure() {
		Map<Slot, Key<?>> slots = new HashMap<>();
		transform(0, (bindings, scope, key, binding) -> {
			if (key.getQualifier() instanceof Slot) {
				slots.put((Slot) key.getQualifier(), key);
			}
			return binding;
		});

		// when someone provides @Slot("id") T, but we request just @Slot("id") Object
		generate(Object.class, (bindings, scope, key) -> {
			if (!(key.getQualifier() instanceof Slot)) {
				return null;
			}
			@SuppressWarnings("SuspiciousMethodCalls") // we've checked it just above
					Key<?> mapped = slots.get(key.getQualifier());
			return mapped != null ? Binding.to(mapped) : null;
		});
	}

	public static Key<Object> slot(String id) {
		return Key.of(Object.class, new SlotImpl(id));
	}

	@QualifierAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Slot {
		String value();
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class SlotImpl implements Slot {
		@NotNull
		private final String value;

		SlotImpl(@NotNull String value) {
			this.value = value;
		}

		@NotNull
		@Override
		public String value() {
			return value;
		}

		@Override
		public int hashCode() {
			// This is specified in java.lang.Annotation.
			return (127 * "value".hashCode()) ^ value.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Slot)) return false;

			Slot other = (Slot) o;
			return value.equals(other.value());
		}

		@NotNull
		@Override
		public String toString() {
			return "@" + Slot.class.getName() + "(" + value + ")";
		}

		@NotNull
		@Override
		public Class<? extends Annotation> annotationType() {
			return Slot.class;
		}
	}
}
