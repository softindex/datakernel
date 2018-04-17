package io.datakernel.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class AttributeNodeForConverterType<T> extends AttributeNodeForLeafAbstract {
	private static final Logger logger = LoggerFactory.getLogger(AttributeNodeForConverterType.class);

	private Method setter;
	private Function<T, String> to;
	private Function<String, T> from;

	public AttributeNodeForConverterType(String name, String description, ValueFetcher fetcher,
										 boolean visible, Method setter,
										 Function<T, String> to, Function<String, T> from) {
		super(name, description, fetcher, visible);
		this.setter = setter;
		this.to = to;
		this.from = from;
	}

	public AttributeNodeForConverterType(String name, String description, boolean visible,
										 ValueFetcher fetcher,
										 Method setter,
										 Function<T, String> to, Function<String, T> from) {
		this(name, description, fetcher, visible, setter, to, from);
	}

	public AttributeNodeForConverterType(String name, String description, boolean visible,
										 ValueFetcher fetcher,
										 Method setter,
										 Function<T, String> to) {
		this(name, description, fetcher, visible, setter, to, null);
	}

	@Override
	protected Object aggregateAttribute(String attrName, List<?> sources) {
		Object firstPojo = sources.get(0);
		Object firstValue = (fetcher.fetchFrom(firstPojo));
		if (firstValue == null) {
			return null;
		}

		for (int i = 1; i < sources.size(); i++) {
			Object currentPojo = sources.get(i);
			Object currentValue = Objects.toString(fetcher.fetchFrom(currentPojo));
			if (!Objects.equals(firstPojo, currentValue)) {
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
		Object source = targets.get(0);
		T result;

		if (isSettable("")) {
			result = from.apply((String) value);
		} else {
			throw new SetterException(new IllegalAccessException("Cannot set non writable attribute " + name));
		}

		try {
			setter.invoke(source, result);
		} catch (Exception exception) {
			logger.error("Can't set attribute " + attrName, exception);
		}
	}
}
