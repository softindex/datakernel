package io.datakernel.eventloop.jmx;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.api.JmxBeanAdapterWithRefresh;
import io.datakernel.jmx.api.JmxRefreshable;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.common.Preconditions.*;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static java.lang.System.identityHashCode;

public final class EventloopJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Map<IdentityKey, Eventloop> beanToEventloop = new ConcurrentHashMap<>();

	private static final class IdentityKey {
		private final Object object;

		private IdentityKey(Object object) {this.object = object;}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			IdentityKey that = (IdentityKey) o;
			return this.object == that.object;
		}

		@Override
		public int hashCode() {
			return identityHashCode(object);
		}
	}

	private Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	public void execute(Object bean, Runnable command) {
		Eventloop eventloop = ensureEventloop(bean);
		eventloop.execute(wrapContext(bean, command));
	}

	private Eventloop ensureEventloop(Object bean) {
		Eventloop eventloop = beanToEventloop.get(new IdentityKey(bean));
		if (eventloop == null) {
			try {
				eventloop = (Eventloop) bean.getClass().getMethod("getEventloop").invoke(bean);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new IllegalStateException("Class annotated with @EventloopJmxBean should have a 'getEventloop()' method");
			}
			checkNotNull(eventloop);
			beanToEventloop.put(new IdentityKey(bean), eventloop);
		}
		return eventloop;
	}

	@Override
	public void init(@NotNull Duration refreshPeriod, int maxRefreshesPerCycle) {
		checkArgument(refreshPeriod.toMillis() > 0);
		checkArgument(maxRefreshesPerCycle > 0);
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
	}

	@Override
	synchronized public void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");

		Eventloop eventloop = ensureEventloop(bean);
		if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
			List<JmxRefreshable> list = new ArrayList<>();
			eventloopToJmxRefreshables.put(eventloop, list);
			eventloop.execute(wrapContext(this, () -> refresh(eventloop, list, 0)));
		}

		eventloop.submit(() -> {
			List<JmxRefreshable> refreshables = eventloopToJmxRefreshables.get(eventloop);
			refreshables.addAll(beanRefreshables);
		});
	}

	@Override
	public Duration getEffectiveRefreshPeriod() {
		return Duration.ofMillis(eventloopToJmxRefreshables.values().stream()
				.map(List::size)
				.mapToLong(this::computeEffectiveRefreshPeriod)
				.max()
				.orElse(0));
	}

	private void refresh(@NotNull Eventloop eventloop, @NotNull List<JmxRefreshable> list, int startIndex) {
		checkState(eventloop.inEventloopThread());

		long currentTime = eventloop.currentTimeMillis();

		int index = startIndex < list.size() ? startIndex : 0;
		int endIndex = Math.min(list.size(), index + maxRefreshesPerCycle);
		while (index < endIndex) {
			list.get(index++).refresh(currentTime);
		}

		long nextTimestamp = currentTime + computeEffectiveRefreshPeriod(list.size());
		eventloop.scheduleBackground(nextTimestamp, wrapContext(this, () -> refresh(eventloop, list, endIndex)));
	}

	private long computeEffectiveRefreshPeriod(int totalCount) {
		return maxRefreshesPerCycle >= totalCount ?
				refreshPeriod.toMillis() :
				refreshPeriod.toMillis() * maxRefreshesPerCycle / totalCount;
	}

}
