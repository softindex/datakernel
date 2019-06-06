package io.datakernel.jmx;

import org.jetbrains.annotations.Nullable;

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("unchecked")
public class AttributeNodeForConverterType<T> extends AttributeNodeForLeafAbstract {
	private static final Logger logger = Logger.getLogger(AttributeNodeForConverterType.class.getName());

	@Nullable
	private Method setter;
	private Function<T, String> to;
	@Nullable
	private Function<String, T> from;

	public AttributeNodeForConverterType(String name, @Nullable String description, ValueFetcher fetcher,
			boolean visible, @Nullable Method setter,
			Function<T, String> to, @Nullable Function<String, T> from) {
		super(name, description, fetcher, visible);
		this.setter = setter;
		this.to = to;
		this.from = from;
	}

	public AttributeNodeForConverterType(String name, @Nullable String description, boolean visible,
			ValueFetcher fetcher, @Nullable Method setter,
			Function<T, String> to, @Nullable Function<String, T> from) {
		this(name, description, fetcher, visible, setter, to, from);
	}

	public AttributeNodeForConverterType(String name, String description, boolean visible,
			ValueFetcher fetcher, Method setter,
			Function<T, String> to) {
		this(name, description, fetcher, visible, setter, to, null);
	}

	@Override
	@Nullable
	protected Object aggregateAttribute(String attrName, List<?> sources) {
		Object firstPojo = sources.get(0);
		Object firstValue = fetcher.fetchFrom(firstPojo);
		if (firstValue == null) {
			return null;
		}

		for (int i = 1; i < sources.size(); i++) {
			Object currentPojo = sources.get(i);
			Object currentValue = fetcher.fetchFrom(currentPojo);
			if (!Objects.equals(firstValue, currentValue)) {
				return null;
			}
		}
		return to.apply((T) firstValue);
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Collections.singletonMap(name, SimpleType.STRING);
	}

	@Override
	public List<JmxRefreshable> getAllRefreshables(Object source) {
		return Collections.emptyList();
	}

	@Override
	public boolean isSettable(String attrName) {
		return setter != null && from != null;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) throws SetterException {
		if (!isSettable("")) {
			throw new SetterException(new IllegalAccessException("Cannot set non writable attribute " + name));
		}
		assert from != null && setter != null; // above settable check
		T result = from.apply((String) value);
		for (Object target : targets) {
			try {
				setter.invoke(target, result);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Can't set attribute " + attrName, e);
			}
		}
	}
}
