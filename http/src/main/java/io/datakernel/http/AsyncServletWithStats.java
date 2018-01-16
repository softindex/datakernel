package io.datakernel.http;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;

import java.util.concurrent.CompletionStage;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxMBean {
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	protected final Eventloop eventloop;

	private final StageStats stats = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	protected AsyncServletWithStats(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected abstract CompletionStage<HttpResponse> doServe(HttpRequest request);

	@Override
	public final CompletionStage<HttpResponse> serve(HttpRequest request) {
		return stats.monitor(this::doServe, request);
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

	@JmxAttribute
	public double getStatsSmoothingWindow() {
		return stats.getSmoothingWindow();
	}

	@JmxAttribute
	public void setStatsSmoothingWindow(double smoothingWindowSeconds) {
		stats.setSmoothingWindow(smoothingWindowSeconds);
	}

	@JmxOperation
	public void resetStats() {
		stats.resetStats();
	}

}
