package io.datakernel.eventloop;

import io.datakernel.common.Stopwatch;
import io.datakernel.common.inspector.BaseInspector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface EventloopInspector extends BaseInspector<EventloopInspector> {
	void onUpdateBusinessLogicTime(boolean taskOrKeyPresent, boolean externalTaskPresent, long businessLogicTime);

	void onUpdateSelectorSelectTime(long selectorSelectTime);

	void onUpdateSelectorSelectTimeout(long selectorSelectTimeout);

	void onUpdateSelectedKeyDuration(@NotNull Stopwatch sw);

	void onUpdateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys, long loopTime);

	void onUpdateLocalTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw);

	void onUpdateLocalTasksStats(int localTasks, long loopTime);

	void onUpdateConcurrentTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw);

	void onUpdateConcurrentTasksStats(int newConcurrentTasks, long loopTime);

	void onUpdateScheduledTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw, boolean background);

	void onUpdateScheduledTasksStats(int scheduledTasks, long loopTime, boolean background);

	void onFatalError(@NotNull Throwable e, @Nullable Object context);

	void onScheduledTaskOverdue(int overdue, boolean background);
}
