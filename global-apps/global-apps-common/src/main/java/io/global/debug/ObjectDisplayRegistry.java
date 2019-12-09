package io.global.debug;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import org.jetbrains.annotations.NotNull;

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
	public String getShortDisplay(@NotNull Object object) {
		ObjectDisplay<Object> shortDisplay = (ObjectDisplay<Object>) shortDisplays.getOrDefault(object.getClass(), DEFAULT_SHORT_DISPLAY);
		return shortDisplay.display(this, object);
	}

	@SuppressWarnings("unchecked")
	public String getLongDisplay(@NotNull Object object) {
		ObjectDisplay<Object> longDisplay = (ObjectDisplay<Object>) longDisplays.getOrDefault(object.getClass(), DEFAULT_LONG_DISPLAY);
		return longDisplay.display(this, object);
	}

	@FunctionalInterface
	public interface ObjectDisplay<T> {

		String display(ObjectDisplayRegistry registry, @NotNull T object);
	}
}
