package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;

import java.util.concurrent.CompletionStage;

public class StageWatcher implements EventloopJmxMBean {
	private final Eventloop eventloop;

	private long lastDuration = 0;
	private long lastStartTimestamp = 0;
	private long lastResultTimestamp = 0;
	private ExceptionStats exceptionStats = ExceptionStats.create();

	public StageWatcher(final Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public <T> CompletionStage<T> watch(CompletionStage<T> callable) {
		lastStartTimestamp = eventloop.currentTimeMillis();
		return callable.whenComplete(Stages.onResult(t -> lastResultTimestamp = eventloop.currentTimeMillis()))
				.whenComplete(Stages.onError(exceptionStats::recordException))
				.whenComplete((t, throwable) -> {
					lastDuration = eventloop.currentTimeMillis() - lastStartTimestamp;
					lastStartTimestamp = 0;
				});
	}

	@JmxAttribute
	public long getLastDuration() {
		return lastDuration;
	}

	@JmxAttribute
	public long getLastStartTimestamp() {
		return lastStartTimestamp;
	}

	@JmxAttribute
	public long getLastResultTimestamp() {
		return lastResultTimestamp;
	}

	@JmxAttribute
	public String getLastResultTime() {
		return lastResultTimestamp == 0
				? "No result"
				: MBeanFormat.formatPeriodAgo(lastResultTimestamp);
	}

	@JmxAttribute
	public ExceptionStats getExceptionStats() {
		return exceptionStats;
	}

	@JmxOperation
	public void resetStats() {
		exceptionStats.resetStats();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}