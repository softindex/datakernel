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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class RefreshTaskPerMBean implements Runnable {
	private final List<? extends JmxStats<?>> statsList;
	private final Timer timer;
	private final TimerTask nextRefreshTask;
	private final AtomicInteger localTasksLeft;
	private final double smoothingWindow;
	private final int delayToNextRefreshInMillis;
	private final long currentTimestamp;

	public RefreshTaskPerMBean(List<? extends JmxStats<?>> statsList, Timer timer,
	                           TimerTask nextRefreshTask, AtomicInteger localTasksLeft,
	                           double smoothingWindow, int delayToNextRefreshInMillis,
	                           long currentTimestamp) {
		this.statsList = statsList;
		this.timer = timer;
		this.nextRefreshTask = nextRefreshTask;
		this.localTasksLeft = localTasksLeft;
		this.smoothingWindow = smoothingWindow;
		this.delayToNextRefreshInMillis = delayToNextRefreshInMillis;
		this.currentTimestamp = currentTimestamp;
	}

	@Override
	public void run() {
		System.out.println("ringo");
		for (JmxStats<?> jmxStats : statsList) {
			jmxStats.refreshStats(currentTimestamp, smoothingWindow);
		}
		int tasksLeft = localTasksLeft.decrementAndGet();
		if (tasksLeft == 0) {
			timer.schedule(nextRefreshTask, delayToNextRefreshInMillis);
		}
	}
}
