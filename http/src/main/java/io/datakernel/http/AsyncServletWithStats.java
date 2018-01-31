package io.datakernel.http;

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxMBeanEx {
	protected final Eventloop eventloop;

	private final StageStats stats = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);

	protected AsyncServletWithStats(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected abstract Stage<HttpResponse> doServe(HttpRequest request);

	@Override
	public final Stage<HttpResponse> serve(HttpRequest request) {
		return doServe(request).whenComplete(stats.recordStats());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public StageStats getStats() {
		return stats;
	}

	public void setStatsHistogramLevels(int[] levels) {
		stats.setHistogramLevels(levels);
	}

}
