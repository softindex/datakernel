package io.datakernel.jmx.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

public interface JmxBeanAdapterWithRefresh extends JmxBeanAdapter {
	void init(Duration refreshPeriod, int maxRefreshesPerCycle);

	void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables);

	Collection<Integer> getRefreshableStatsCounts();

	Collection<Integer> getEffectiveRefreshPeriods();
}
