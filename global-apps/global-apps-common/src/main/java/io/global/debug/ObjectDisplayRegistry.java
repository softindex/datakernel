package io.global.debug;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class ObjectDisplayRegistry {
	private static final ObjectDisplay<?> DEFAULT_SHORT_DISPLAY = ($, o) -> o.getClass().getSimpleName();
	private static final ObjectDisplay<?> DEFAULT_LONG_DISPLAY = ($, o) -> o.toString();

	private Map<Type, ObjectDisplay<?>> shortDisplays = new HashMap<>();
	private Map<Type, ObjectDisplay<?>> longDisplays = new HashMap<>();

	// region builders
	private ObjectDisplayRegistry() {
	}

	@Inject
	public static ObjectDisplayRegistry create() {
		return new ObjectDisplayRegistry();
	}

	public static ObjectDisplayRegistry merge(ObjectDisplayRegistry first, ObjectDisplayRegistry second) {
		ObjectDisplayRegistry result = new ObjectDisplayRegistry();
		result.shortDisplays.putAll(first.shortDisplays);
		result.longDisplays.putAll(first.longDisplays);
		result.shortDisplays.putAll(second.shortDisplays);
		result.longDisplays.putAll(second.longDisplays);
		return result;
	}

	public <T> ObjectDisplayRegistry withDisplay(Class<T> diffType, ObjectDisplay<T> shortPrinter, ObjectDisplay<T> longPrinter) {
		shortDisplays.put(diffType, shortPrinter);
		longDisplays.put(diffType, longPrinter);
		return this;
	}

	public <T> ObjectDisplayRegistry withDisplay(Key<T> diffType, ObjectDisplay<T> shortPrinter, ObjectDisplay<T> longPrinter) {
		shortDisplays.put(diffType.getType(), shortPrinter);
		longDisplays.put(diffType.getType(), longPrinter);
		return this;
	}

	public <T> ObjectDisplayRegistry withLongDisplay(Class<T> diffType, ObjectDisplay<T> printer) {
		longDisplays.put(diffType, printer);
	    return this;
	}

	public <T> ObjectDisplayRegistry withLongDisplay(Key<T> diffType, ObjectDisplay<T> printer) {
		longDisplays.put(diffType.getType(), printer);
		return this;
	}

	public <T> ObjectDisplayRegistry withShortDisplay(Class<T> diffType, ObjectDisplay<T> printer) {
		shortDisplays.put(diffType, printer);
		return this;
	}

	public <T> ObjectDisplayRegistry withShortDisplay(Key<T> diffType, ObjectDisplay<T> printer) {
		shortDisplays.put(diffType.getType(), printer);
		return this;
	}
	// endregion

	@SuppressWarnings("unchecked")
	private String getDisplay(Map<Type, ObjectDisplay<?>> displayMap, Type type, @Nullable Object instance, ObjectDisplay<?> defaultDisplay) {
		if (instance == null) {
			return "null";
		}
		ObjectDisplay<?> display = displayMap.get(type);
		if (display == null) {
			display = displayMap.get(instance.getClass());
		}
		if (display == null) {
			display = defaultDisplay;
		}
		return ((ObjectDisplay<Object>) display).display(this, instance);
	}

	public <T> String getShortDisplay(Key<? extends T> type, @Nullable T instance) {
		return getDisplay(shortDisplays, type.getType(), instance, DEFAULT_SHORT_DISPLAY);
	}

	public <T> String getLongDisplay(Key<? extends T> type, @Nullable T instance) {
		return getDisplay(longDisplays, type.getType(), instance, DEFAULT_LONG_DISPLAY);
	}

	public String getShortDisplay(@Nullable Object object) {
		return object == null ? "null" : getShortDisplay(Key.of(object.getClass()), object);
	}

	public String getLongDisplay(@Nullable Object object) {
		return object == null ? "null" : getLongDisplay(Key.of(object.getClass()), object);
	}

	@FunctionalInterface
	public interface ObjectDisplay<T> {

		String display(ObjectDisplayRegistry registry, @NotNull T object);
	}
}
