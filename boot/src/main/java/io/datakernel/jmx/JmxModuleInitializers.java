package io.datakernel.jmx;

import com.google.inject.Key;
import com.google.inject.name.Names;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.Initializer;

public class JmxModuleInitializers {
	private JmxModuleInitializers() {
	}

	public static final String GLOBAL_EVENTLOOP_NAME = "GlobalEventloopStats";
	public static final Key<Eventloop> GLOBAL_EVENTLOOP_KEY = Key.get(Eventloop.class, Names.named(GLOBAL_EVENTLOOP_NAME));

	public static Initializer<JmxModule> ofGlobalEventloopStats() {
		return jmxModule -> jmxModule
				.withGlobalMBean(Eventloop.class, GLOBAL_EVENTLOOP_KEY)
				.withOptional(GLOBAL_EVENTLOOP_KEY, "fatalErrors_total")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "loops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "businessLogicTime_smoothedAverage")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_totalCount");
	}
}
