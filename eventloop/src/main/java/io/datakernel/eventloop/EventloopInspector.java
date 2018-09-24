package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.util.Stopwatch;

public interface EventloopInspector {
	void onUpdateBusinessLogicTime(boolean taskOrKerPresent, boolean externalTaskPresent, long businessLogicTime);

	void onUpdateSelectorSelectTime(long selectorSelectTime);

	void onUpdateSelectorSelectTimeout(long selectorSelectTimeout);

	void onUpdateSelectedKeyDuration(Stopwatch sw);

	void onUpdateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys, long loopTime);

	void onUpdateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw);

	void onUpdateLocalTasksStats(int newLocalTasks, long loopTime);

	void onUpdateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw);

	void onUpdateConcurrentTasksStats(int newConcurrentTasks, long loopTime);

	void onUpdateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw, boolean background);

	void onUpdateScheduledTasksStats(int newScheduledTasks, long loopTime, boolean background);

	void onFatalError(Throwable throwable, Object causedObject);

	void onScheduledTaskOverdue(int overdue, boolean background);

	void setEventloop(Eventloop eventloop);
}
