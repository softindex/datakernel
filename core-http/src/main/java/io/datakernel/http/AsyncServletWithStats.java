package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;

import java.time.Duration;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxMBeanEx {
	protected final Eventloop eventloop;

	private final PromiseStats stats = PromiseStats.create(Duration.ofMinutes(5));

	protected AsyncServletWithStats(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected abstract Promise<HttpResponse> doServe(HttpRequest request);

	@Override
	public final Promise<HttpResponse> serve(HttpRequest request) {
		return doServe(request).whenComplete(stats.recordStats());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public PromiseStats getStats() {
		return stats;
	}

	public void setStatsHistogramLevels(int[] levels) {
		stats.setHistogramLevels(levels);
	}

}
