package io.datakernel.jmx;

import io.datakernel.eventloop.Eventloop;

import java.util.concurrent.CompletionStage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.jmx.JmxReducers.JmxReducerMax;
import static io.datakernel.jmx.JmxReducers.JmxReducerSum;
import static io.datakernel.jmx.MBeanFormat.formatPeriodAgo;

public class StageStats implements EventloopJmxMBean {
	public static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_5_MINUTES;

	private Eventloop eventloop;

	private boolean monitoring = true;
	private int activeStages = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private long lastExceptionTimestamp = 0;
	private final ValueStats duration = ValueStats
			.create(SMOOTHING_WINDOW)
			.withHistogram(ValueStats.POWERS_OF_TEN_SEMI_LINEAR);
	private final ExceptionStats exceptions = ExceptionStats.create();

	private StageStats(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static StageStats createMBean(Eventloop eventloop) {
		return new StageStats(eventloop);
	}

	public static StageStats create() {
		return new StageStats(null);
	}

	public StageStats withSmoothingWindow(double smoothingWindowSeconds) {
		setSmoothingWindow(smoothingWindowSeconds);
		return this;
	}

	public StageStats withHistogram(int[] levels) {
		setHistogramLevels(levels);
		return this;
	}

	public StageStats withMonitoring(boolean monitoring) {
		setMonitoring(monitoring);
		return this;
	}

	public void setHistogramLevels(int[] levels) {
		duration.setHistogramLevels(levels);
	}

	private long currentTimeMillis() {
		if (eventloop == null) {
			eventloop = getCurrentEventloop();
		}
		return eventloop.currentTimeMillis();
	}

	public <T> CompletionStage<T> monitor(CompletionStage<T> stage) {
		if (!monitoring) return stage;
		this.activeStages++;
		long before = currentTimeMillis();
		this.lastStartTimestamp = before;
		return stage
				.whenComplete((t, throwable) -> {
					this.activeStages--;
					long now = currentTimeMillis();
					long durationMillis = now - before;
					this.lastCompleteTimestamp = now;
					duration.recordValue(durationMillis);

					if (throwable != null) {
						exceptions.recordException(throwable);
						lastExceptionTimestamp = now;
					}
				});
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getActiveStages() {
		return activeStages;
	}

	@JmxAttribute(reducer = JmxReducerMax.class)
	public long getLastStartTimestamp() {
		return lastStartTimestamp;
	}

	@JmxAttribute
	public String getLastStartTime() {
		return lastStartTimestamp != 0 ? formatPeriodAgo(lastStartTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class)
	public long getLastCompleteTimestamp() {
		return lastCompleteTimestamp;
	}

	@JmxAttribute
	public String getLastCompleteTime() {
		return lastCompleteTimestamp != 0 ? formatPeriodAgo(lastCompleteTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class)
	public long getLastExceptionTimestamp() {
		return lastExceptionTimestamp;
	}

	@JmxAttribute
	public String getLastExceptionTime() {
		return lastExceptionTimestamp != 0 ? formatPeriodAgo(lastExceptionTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class)
	public long getCurrentDuration() {
		return activeStages != 0 ? currentTimeMillis() - lastStartTimestamp : 0;
	}

	@JmxAttribute
	public ValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}

	@JmxAttribute(optional = true)
	public double getSmoothingWindow() {
		return duration.getSmoothingWindow();
	}

	@JmxAttribute(optional = true)
	public void setSmoothingWindow(double smoothingWindowSeconds) {
		duration.setSmoothingWindow(smoothingWindowSeconds);
	}

	@JmxOperation
	public void resetStats() {
		duration.resetStats();
		exceptions.resetStats();
	}

	@JmxAttribute(optional = true)
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(optional = true)
	public void setMonitoring(boolean monitoring) {
		this.monitoring = monitoring;
	}
}