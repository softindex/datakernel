/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.jmx;

import io.datakernel.time.CurrentTimeProvider;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public final class RefreshTaskPerPool extends TimerTask {
	private final List<ExecutorAndStatsList> executorsAndStatsList;
	private final CurrentTimeProvider currentTimeProvider;
	private final Timer timer;
	private volatile double smoothingWindow;
	private volatile int refreshPeriodInMillis;

	public RefreshTaskPerPool(List<ExecutorAndStatsList> executorsAndStatsList, CurrentTimeProvider currentTimeProvider,
	                          Timer timer, double smoothingWindow, double refreshPeriod) {
		this.executorsAndStatsList = executorsAndStatsList;
		this.currentTimeProvider = currentTimeProvider;
		this.timer = timer;
		this.smoothingWindow = smoothingWindow;
		this.refreshPeriodInMillis = secondsToMillis(refreshPeriod);
	}

	@Override
	public void run() {
		AtomicInteger localTasks = new AtomicInteger(executorsAndStatsList.size());
		// cache smoothingWindow and refreshPeriod to be same for all localRefreshTasks
		// because this two parameters may be changed from other thread
		double currentSmoothingWindow = smoothingWindow;
		int currentRefreshPeriod = refreshPeriodInMillis;
		long currentTimeStamp = currentTimeProvider.currentTimeMillis();
		for (ExecutorAndStatsList executorAndStatsList : executorsAndStatsList) {
			Executor executor = executorAndStatsList.getExecutor();
			List<? extends JmxStats<?>> statsList = executorAndStatsList.getStatsList();
			RefreshTaskPerMBean refreshTaskPerMBean = new RefreshTaskPerMBean(statsList, timer, this, localTasks,
					currentSmoothingWindow, currentRefreshPeriod, currentTimeStamp);
			executor.execute(refreshTaskPerMBean);
		}
	}

	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
	}

	public void setRefreshPeriod(double refreshPeriod) {
		this.refreshPeriodInMillis = secondsToMillis(refreshPeriod);
	}

	public double getSmoothingWindow() {
		return smoothingWindow;
	}

	public double getRefreshPeriod() {
		return millisToSeconds(refreshPeriodInMillis);
	}

	private static int secondsToMillis(double seconds) {
		return (int) (seconds * 1000);
	}

	private static double millisToSeconds(int millis) {
		return millis / 1000.0;
	}

	public static final class ExecutorAndStatsList {
		private final Executor executor;
		private final List<? extends JmxStats<?>> statsList;

		public ExecutorAndStatsList(Executor executor, List<? extends JmxStats<?>> statsList) {
			this.executor = executor;
			this.statsList = statsList;
		}

		public Executor getExecutor() {
			return executor;
		}

		public List<? extends JmxStats<?>> getStatsList() {
			return statsList;
		}
	}
}
