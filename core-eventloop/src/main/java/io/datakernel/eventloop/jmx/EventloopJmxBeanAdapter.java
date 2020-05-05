package io.datakernel.eventloop.jmx;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.api.JmxBeanAdapterWithRefresh;
import io.datakernel.jmx.api.JmxRefreshable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static java.lang.Math.ceil;

public final class EventloopJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> refreshableStatsCounts = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> effectiveRefreshPeriods = new ConcurrentHashMap<>();
	private final AtomicReference<IdentityHashMap<Object, Eventloop>> beanToEventloop = new AtomicReference<>(new IdentityHashMap<>());

	private Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	public void execute(Object bean, Runnable command) {
		Eventloop eventloop = ensureEventloop(bean);
		eventloop.execute(wrapContext(bean, command));
	}

	private Eventloop ensureEventloop(Object bean) {
		Eventloop eventloop = beanToEventloop.get().get(bean);
		if (eventloop != null) return eventloop;
		try {
			eventloop = (Eventloop) bean.getClass().getMethod("getEventloop").invoke(bean);
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new IllegalStateException("Class annotated with @EventloopJmxMBean should have a 'getEventloop()' method");
		}
		checkNotNull(eventloop);
		while (true) {
			if (beanToEventloop.get().containsKey(bean)) break;
			IdentityHashMap<Object, Eventloop> oldMap = beanToEventloop.get();
			IdentityHashMap<Object, Eventloop> newMap = new IdentityHashMap<>(oldMap);
			newMap.put(bean, eventloop);
			if (beanToEventloop.compareAndSet(oldMap, newMap)) break;
		}
		return eventloop;
	}

	@Override
	public void init(@NotNull Duration refreshPeriod, int maxRefreshesPerCycle) {
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
	}

	@Override
	public void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");
		Eventloop eventloop = ensureEventloop(bean);
		if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
			eventloopToJmxRefreshables.put(eventloop, beanRefreshables);
			eventloop.execute(wrapContext(this, createRefreshTask(eventloop, null, 0)));
		} else {
			List<JmxRefreshable> previousRefreshables = eventloopToJmxRefreshables.get(eventloop);
			List<JmxRefreshable> allRefreshables = new ArrayList<>(previousRefreshables);
			allRefreshables.addAll(beanRefreshables);
			eventloopToJmxRefreshables.put(eventloop, allRefreshables);
		}

		refreshableStatsCounts.put(eventloop, eventloopToJmxRefreshables.get(eventloop).size());
	}

	@Override
	public Collection<Integer> getRefreshableStatsCounts() {
		return refreshableStatsCounts.values();
	}

	@Override
	public Collection<Integer> getEffectiveRefreshPeriods() {
		return effectiveRefreshPeriods.values();
	}

	private Runnable createRefreshTask(Eventloop eventloop, @Nullable List<JmxRefreshable> previousList, int previousRefreshes) {
		return () -> {
			long currentTime = eventloop.currentTimeMillis();

			List<JmxRefreshable> jmxRefreshableList = previousList;
			if (jmxRefreshableList == null) {
				// list might be updated in case of several mbeans in one eventloop
				jmxRefreshableList = eventloopToJmxRefreshables.get(eventloop);
				effectiveRefreshPeriods.put(eventloop, (int) computeEffectiveRefreshPeriod(jmxRefreshableList.size()));
			}

			int currentRefreshes = 0;
			while (currentRefreshes < maxRefreshesPerCycle) {
				int index = currentRefreshes + previousRefreshes;
				if (index == jmxRefreshableList.size()) {
					break;
				}
				jmxRefreshableList.get(index).refresh(currentTime);
				currentRefreshes++;
			}

			long nextTimestamp = currentTime + computeEffectiveRefreshPeriod(jmxRefreshableList.size());
			int totalRefreshes = currentRefreshes + previousRefreshes;
			if (totalRefreshes == jmxRefreshableList.size()) {
				eventloop.scheduleBackground(nextTimestamp, wrapContext(this, createRefreshTask(eventloop, null, 0)));
			} else {
				eventloop.scheduleBackground(nextTimestamp, wrapContext(this, createRefreshTask(eventloop, jmxRefreshableList, totalRefreshes)));
			}
		};
	}

	private long computeEffectiveRefreshPeriod(int jmxRefreshablesCount) {
		if (jmxRefreshablesCount == 0) {
			return refreshPeriod.toMillis();
		}
		double ratio = ceil(jmxRefreshablesCount / (double) maxRefreshesPerCycle);
		return (long) (refreshPeriod.toMillis() / ratio);
	}

}
