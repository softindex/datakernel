package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxMBeanEx {
	@NotNull
	protected final Eventloop eventloop;

	private final PromiseStats stats = PromiseStats.create(Duration.ofMinutes(5));

	protected AsyncServletWithStats(@NotNull Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@NotNull
	protected abstract Promise<HttpResponse> doServe(@NotNull HttpRequest request);

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return doServe(request)
				.whenComplete(stats.recordStats());
	}

	@NotNull
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
