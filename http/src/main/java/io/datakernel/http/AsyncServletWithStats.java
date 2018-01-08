package io.datakernel.http;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;

import java.util.concurrent.CompletionStage;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxMBean {
	protected final Eventloop eventloop;

	private final StageStats stats = StageStats.create();
	private final ExceptionStats fatalErrors = ExceptionStats.create();

	protected AsyncServletWithStats(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected abstract CompletionStage<HttpResponse> doServe(HttpRequest request);

	@Override
	public final CompletionStage<HttpResponse> serve(HttpRequest request) {
		try {
			CompletionStage<HttpResponse> stage = doServe(request);
			return stats.monitor(stage);
		} catch (Throwable throwable) {
			fatalErrors.recordException(throwable, request);
			throw throwable;
		}
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

	@JmxAttribute
	public boolean isStatsMonitoring() {
		return stats.isMonitoring();
	}

	@JmxAttribute
	public void setStatsMonitoring(boolean monitoring) {
		stats.setMonitoring(monitoring);
	}

	@JmxOperation
	public void resetStats() {
		stats.resetStats();
	}

}
