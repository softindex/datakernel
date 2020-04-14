package io.datakernel.jmx.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public interface JmxRefreshHandler {

	void init(Duration refreshPeriod, int maxRefreshesPerCycle);

	void handleRefresh(List<MBeanWrapper> wrappers, Function<Object, List<JmxRefreshable>> refreshablesExtractor);

	Collection<Integer> getRefreshableStatsCounts();

	Collection<Integer> getEffectiveRefreshPeriods();
}
