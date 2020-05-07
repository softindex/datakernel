package io.datakernel.eventloop.jmx;

import io.datakernel.common.tuple.Tuple2;
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
import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

public final class EventloopJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	private final Map<Eventloop, Tuple2<ValueStats, List<JmxRefreshable>>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Set<JmxRefreshable> allRefreshables = Collections.newSetFromMap(new IdentityHashMap<>());
	private final Map<Object, Eventloop> beanToEventloop = new IdentityHashMap<>();

	private Duration refreshPeriod;
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
		for (Map.Entry<Eventloop, Tuple2<ValueStats, List<JmxRefreshable>>> entry : eventloopToJmxRefreshables.entrySet()) {
			entry.getKey().execute(() -> entry.getValue().getValue1().resetStats());
		}
	}

	@Override
	synchronized public void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");

		Eventloop eventloop = ensureEventloop(bean);
		if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
			Duration smoothingWindows = eventloop.getSmoothingWindow();
			if (smoothingWindows == null){
				smoothingWindows = DEFAULT_SMOOTHING_WINDOW;
			}
			ValueStats refreshStats = ValueStats.create(smoothingWindows).withRate().withUnit("ms");
			List<JmxRefreshable> list = new ArrayList<>();
			list.add(refreshStats);
			eventloopToJmxRefreshables.put(eventloop, new Tuple2<>(refreshStats, list));
			eventloop.execute(wrapContext(this, () -> refresh(eventloop, list, 0)));
		}

		Set<JmxRefreshable> beanRefreshablesFiltered = new HashSet<>();
		for (JmxRefreshable refreshable : beanRefreshables) {
			if (allRefreshables.add(refreshable)) {
				beanRefreshablesFiltered.add(refreshable);
			}
		}

		eventloop.submit(() -> {
			List<JmxRefreshable> refreshables = eventloopToJmxRefreshables.get(eventloop).getValue2();
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
	public String[] getRefreshStats() {
		int size = eventloopToJmxRefreshables.size();
		if (size == 0) return new String[0];
		if (size == 1) {
			Tuple2<ValueStats, List<JmxRefreshable>> statsAndRefreshables = first(eventloopToJmxRefreshables.values());
			return new String[]{getStatsString(statsAndRefreshables.getValue2().size(), statsAndRefreshables.getValue1())};
		}

		int count = 0;
		String[] resultStats = new String[size + 1];
		ValueStats accumulator = ValueStats.createAccumulator().withRate().withUnit("ms");
		for (Tuple2<ValueStats, List<JmxRefreshable>> tuple : eventloopToJmxRefreshables.values()) {
			accumulator.add(tuple.getValue1());
			count += tuple.getValue2().size();
		}
		resultStats[0] = getStatsString(count, accumulator);
		int i = 1;
		for (Tuple2<ValueStats, List<JmxRefreshable>> tuple : eventloopToJmxRefreshables.values()) {
			resultStats[i++] = getStatsString(tuple.getValue2().size(), tuple.getValue1());
		}
		return resultStats;
	}

	private void refresh(@NotNull Eventloop eventloop, @NotNull List<JmxRefreshable> list, int startIndex) {
		checkState(eventloop.inEventloopThread());

		long currentTime = eventloop.currentTimeMillis();

		int index = startIndex < list.size() ? startIndex : 0;
		int endIndex = Math.min(list.size(), index + maxRefreshesPerCycle);
		while (index < endIndex) {
			list.get(index++).refresh(currentTime);
		}

		long refreshTime = eventloop.currentTimeMillis() - currentTime;
		eventloopToJmxRefreshables.get(eventloop).getValue1().recordValue(refreshTime);

		long nextTimestamp = currentTime + computeEffectiveRefreshPeriod(list.size());
		eventloop.scheduleBackground(nextTimestamp, wrapContext(this, () -> refresh(eventloop, list, endIndex)));
	}

	private long computeEffectiveRefreshPeriod(int totalCount) {
		return maxRefreshesPerCycle >= totalCount ?
				refreshPeriod.toMillis() :
				refreshPeriod.toMillis() * maxRefreshesPerCycle / totalCount;
	}

	private static String getStatsString(int numberOfRefreshables, ValueStats stats){
		return "# of refreshables: " + numberOfRefreshables + "  " + stats;
	}

}
