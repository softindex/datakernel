package io.datakernel.eventloop.jmx;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.api.JmxBeanAdapterWithRefresh;
import io.datakernel.jmx.api.JmxRefreshable;
import io.datakernel.jmx.stats.ValueStats;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.common.Preconditions.*;
import static io.datakernel.eventloop.util.RunnableWithContext.wrapContext;

public final class EventloopJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Set<JmxRefreshable> allRefreshables = Collections.newSetFromMap(new IdentityHashMap<>());
	private final Map<Object, Eventloop> beanToEventloop = new IdentityHashMap<>();

	private volatile Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	synchronized public void execute(Object bean, Runnable command) {
		Eventloop eventloop = beanToEventloop.get(bean);
		checkNotNull(eventloop, () -> "Unregistered bean " + bean);
		eventloop.execute(wrapContext(bean, command));
	}

	@Override
	public void setRefreshParameters(@NotNull Duration refreshPeriod, int maxRefreshesPerCycle) {
		checkArgument(refreshPeriod.toMillis() > 0);
		checkArgument(maxRefreshesPerCycle > 0);
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
		for (Map.Entry<Eventloop, List<JmxRefreshable>> entry : eventloopToJmxRefreshables.entrySet()) {
			entry.getKey().execute(() -> ((ValueStats) entry.getValue().get(0)).resetStats());
		}
	}

	@Override
	synchronized public void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");

		Eventloop eventloop = ensureEventloop(bean);
		if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
			Duration smoothingWindows = eventloop.getSmoothingWindow();
			if (smoothingWindows == null) {
				smoothingWindows = DEFAULT_SMOOTHING_WINDOW;
			}
			ValueStats refreshStats = ValueStats.create(smoothingWindows).withRate().withUnit("ms");
			List<JmxRefreshable> list = new ArrayList<>();
			list.add(refreshStats);
			eventloopToJmxRefreshables.put(eventloop, list);
			eventloop.execute(wrapContext(this, () -> refresh(eventloop, list, 0, refreshStats)));
		}

		Set<JmxRefreshable> beanRefreshablesFiltered = new HashSet<>();
		for (JmxRefreshable refreshable : beanRefreshables) {
			if (allRefreshables.add(refreshable)) {
				beanRefreshablesFiltered.add(refreshable);
			}
		}

		eventloop.submit(() -> {
			List<JmxRefreshable> refreshables = eventloopToJmxRefreshables.get(eventloop);
			refreshables.addAll(beanRefreshablesFiltered);
		});
	}

	private Eventloop ensureEventloop(Object bean) {
		Eventloop eventloop = beanToEventloop.get(bean);
		if (eventloop == null) {
			try {
				eventloop = (Eventloop) bean.getClass().getMethod("getEventloop").invoke(bean);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new IllegalStateException("Class annotated with @EventloopJmxBean should have a 'getEventloop()' method");
			}
			checkNotNull(eventloop);
			beanToEventloop.put(bean, eventloop);
		}
		return eventloop;
	}

	@Override
	public List<String> getRefreshStats() {
		List<String> result = new ArrayList<>();
		if (eventloopToJmxRefreshables.size() > 1) {
			int count = 0;
			ValueStats total = ValueStats.createAccumulator().withRate().withUnit("ms");
			for (List<JmxRefreshable> refreshables : eventloopToJmxRefreshables.values()) {
				total.add((ValueStats) refreshables.get(0));
				count += refreshables.size();
			}
			result.add(getStatsString(count, total));
		}
		for (List<JmxRefreshable> refreshables : eventloopToJmxRefreshables.values()) {
			result.add(getStatsString(refreshables.size(), (ValueStats) refreshables.get(0)));
		}
		return result;
	}

	private void refresh(@NotNull Eventloop eventloop, @NotNull List<JmxRefreshable> list, int startIndex, ValueStats refreshStats) {
		checkState(eventloop.inEventloopThread());

		long currentTime = eventloop.currentTimeMillis();

		int index = startIndex < list.size() ? startIndex : 0;
		int endIndex = Math.min(list.size(), index + maxRefreshesPerCycle);
		while (index < endIndex) {
			list.get(index++).refresh(currentTime);
		}

		long refreshTime = eventloop.currentTimeMillis() - currentTime;
		refreshStats.recordValue(refreshTime);

		long nextTimestamp = currentTime + computeEffectiveRefreshPeriod(list.size());
		eventloop.scheduleBackground(nextTimestamp, wrapContext(this, () -> refresh(eventloop, list, endIndex, refreshStats)));
	}

	private long computeEffectiveRefreshPeriod(int totalCount) {
		return maxRefreshesPerCycle >= totalCount ?
				refreshPeriod.toMillis() :
				refreshPeriod.toMillis() * maxRefreshesPerCycle / totalCount;
	}

	private static String getStatsString(int numberOfRefreshables, ValueStats stats) {
		return "# of refreshables: " + numberOfRefreshables + "  " + stats;
	}

}
