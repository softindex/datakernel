package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.JmxAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventloopTaskScheduler implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncRunnable task;

	private long period;
	private ScheduledRunnable scheduledTask;

	private EventloopTaskScheduler(Eventloop eventloop, AsyncRunnable task) {
		this.eventloop = eventloop;
		this.task = task;
	}

	public static EventloopTaskScheduler create(Eventloop eventloop, AsyncRunnable task) {
		return new EventloopTaskScheduler(eventloop, task);
	}

	public EventloopTaskScheduler withPeriod(long refreshPeriodMillis) {
		this.period = refreshPeriodMillis;
		return this;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void scheduleTask() {
		if (scheduledTask != null && scheduledTask.isCancelled())
			return;

		if (period == 0)
			return;

		scheduledTask = eventloop.scheduleBackground(
				eventloop.currentTimeMillis() + period,
				new Runnable() {
					@Override
					public void run() {
						task.run(new CompletionCallback() {
							@Override
							protected void onComplete() {
								scheduleTask();
							}

							@Override
							protected void onException(Exception e) {
								logger.error("Task failure", e);
								scheduleTask();
							}
						});
					}
				});
	}

	@Override
	public void start(CompletionCallback callback) {
		scheduleTask();
	}

	@Override
	public void stop(CompletionCallback callback) {
		scheduledTask.cancel();
	}

	@JmxAttribute
	public long getPeriod() {
		return period;
	}

	@JmxAttribute
	public void setPeriod(long refreshPeriodMillis) {
		this.scheduledTask.cancel();
		this.scheduledTask = null;
		this.period = refreshPeriodMillis;
		scheduleTask();
	}

}
