package io.datakernel.jmx.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

public interface JmxRefreshHandler extends JmxWrapperFactory {
	void init(Duration refreshPeriod, int maxRefreshesPerCycle);

	void registerRefreshables(Object bean, List<JmxRefreshable> refreshables);

	Collection<Integer> getRefreshableStatsCounts();

	Collection<Integer> getEffectiveRefreshPeriods();
}
