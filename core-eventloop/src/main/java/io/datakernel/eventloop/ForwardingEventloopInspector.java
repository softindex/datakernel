package io.datakernel.eventloop;

import io.datakernel.common.Stopwatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class ForwardingEventloopInspector implements EventloopInspector {
	protected final @Nullable EventloopInspector next;

	public ForwardingEventloopInspector(@Nullable EventloopInspector next) {this.next = next;}

	@Override
	public void onUpdateBusinessLogicTime(boolean taskOrKeyPresent, boolean externalTaskPresent, long businessLogicTime) {
		if (next != null) next.onUpdateBusinessLogicTime(taskOrKeyPresent, externalTaskPresent, businessLogicTime);
	}

	@Override
	public void onUpdateSelectorSelectTime(long selectorSelectTime) {
		if (next != null) next.onUpdateSelectorSelectTime(selectorSelectTime);
	}

	@Override
	public void onUpdateSelectorSelectTimeout(long selectorSelectTimeout) {
		if (next != null) next.onUpdateSelectorSelectTimeout(selectorSelectTimeout);
	}

	@Override
	public void onUpdateSelectedKeyDuration(@NotNull Stopwatch sw) {
		if (next != null) next.onUpdateSelectedKeyDuration(sw);
	}

	@Override
	public void onUpdateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys, long loopTime) {
		if (next != null) next.onUpdateSelectedKeysStats(lastSelectedKeys, invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys, loopTime);
	}

	@Override
	public void onUpdateLocalTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw) {
		if (next != null) next.onUpdateLocalTaskDuration(runnable, sw);
	}

	@Override
	public void onUpdateLocalTasksStats(int localTasks, long loopTime) {
		if (next != null) next.onUpdateLocalTasksStats(localTasks, loopTime);
	}

	@Override
	public void onUpdateConcurrentTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw) {
		if (next != null) next.onUpdateConcurrentTaskDuration(runnable, sw);
	}

	@Override
	public void onUpdateConcurrentTasksStats(int newConcurrentTasks, long loopTime) {
		if (next != null) next.onUpdateConcurrentTasksStats(newConcurrentTasks, loopTime);
	}

	@Override
	public void onUpdateScheduledTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw, boolean background) {
		if (next != null) next.onUpdateScheduledTaskDuration(runnable, sw, background);
	}

	@Override
	public void onUpdateScheduledTasksStats(int scheduledTasks, long loopTime, boolean background) {
		if (next != null) next.onUpdateScheduledTasksStats(scheduledTasks, loopTime, background);
	}

	@Override
	public void onFatalError(@NotNull Throwable e, @Nullable Object context) {
		if (next != null) next.onFatalError(e, context);
	}

	@Override
	public void onScheduledTaskOverdue(int overdue, boolean background) {
		if (next != null) next.onScheduledTaskOverdue(overdue, background);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends EventloopInspector> @Nullable T lookup(Class<T> type) {
		return type.isAssignableFrom(this.getClass()) ? (T) this :next != null ? next.lookup(type) : null;
	}
}
