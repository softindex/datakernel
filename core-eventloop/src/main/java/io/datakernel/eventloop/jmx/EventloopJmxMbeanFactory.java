package io.datakernel.eventloop.jmx;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.api.JmxRefreshHandler;
import io.datakernel.jmx.api.JmxRefreshable;
import io.datakernel.jmx.api.JmxWrapperFactory;
import io.datakernel.jmx.api.MBeanWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static java.lang.Math.ceil;

public final class EventloopJmxMbeanFactory implements JmxWrapperFactory, JmxRefreshHandler {
	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> refreshableStatsCounts = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> effectiveRefreshPeriods = new ConcurrentHashMap<>();

	private Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	public MBeanWrapper wrap(Object instance) {
		return new EventloopMBeanWrapper(instance);
	}

	@Override
	public void init(@NotNull Duration refreshPeriod, int maxRefreshesPerCycle) {
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
	}

	@Override
	public void handleRefresh(List<MBeanWrapper> wrappers, Function<Object, List<JmxRefreshable>> refreshablesExtractor) {
		checkNotNull(refreshPeriod, "Not initialized");
		for (MBeanWrapper mbeanWrapper : wrappers) {
			Eventloop eventloop = ((EventloopMBeanWrapper) mbeanWrapper).eventloop;
			List<JmxRefreshable> currentRefreshables = refreshablesExtractor.apply(mbeanWrapper.getMBean());
			if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
				eventloopToJmxRefreshables.put(eventloop, currentRefreshables);
				eventloop.execute(wrapContext(this, createRefreshTask(eventloop, null, 0)));
			} else {
				List<JmxRefreshable> previousRefreshables = eventloopToJmxRefreshables.get(eventloop);
				List<JmxRefreshable> allRefreshables = new ArrayList<>(previousRefreshables);
				allRefreshables.addAll(currentRefreshables);
				eventloopToJmxRefreshables.put(eventloop, allRefreshables);
			}

			refreshableStatsCounts.put(eventloop, eventloopToJmxRefreshables.get(eventloop).size());
		}
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

	private static class EventloopMBeanWrapper implements MBeanWrapper {
		private final Eventloop eventloop;
		private final Object instance;

		public EventloopMBeanWrapper(Object instance) {
			this.instance = instance;
			try {
				eventloop = (Eventloop) instance.getClass().getMethod("getEventloop").invoke(instance);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new IllegalStateException("Class annotated with @EventloopJmxMBean should have a 'getEventloop()' method");
			}
		}

		@Override
		public void execute(Runnable command) {
			eventloop.execute(wrapContext(instance, command));
		}

		@Override
		public Object getMBean() {
			return instance;
		}
	}
}
