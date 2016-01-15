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

import io.datakernel.eventloop.Eventloop;

import java.util.List;

public final class StatsRefresher implements Runnable {
	private final Eventloop eventloop;
	private final List<JmxStats<?>> statsList;
	private volatile int periodInMillis;
	private volatile double smoothingWindow;

	/**
	 * Creates StatsRefresher
	 *
	 * @param statsList
	 * @param period          refresh period in seconds
	 * @param smoothingWindow smoothing window in seconds
	 * @param eventloop
	 */
	public StatsRefresher(List<JmxStats<?>> statsList, double period, double smoothingWindow, Eventloop eventloop) {
		this.eventloop = eventloop;
		this.statsList = statsList;
		this.periodInMillis = (int) (period * 1000);
		this.smoothingWindow = smoothingWindow;
	}

	@Override
	public void run() {
		long currentTime = eventloop.currentTimeMillis();
		for (JmxStats<?> stats : statsList) {
			stats.refreshStats(currentTime, smoothingWindow);
		}
		eventloop.scheduleBackground(currentTime + periodInMillis, this);
	}

	public void setPeriod(double period) {
		this.periodInMillis = secondsToMillis(period);
	}

	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
	}

	private static int secondsToMillis(double seconds) {
		return (int) (seconds * 1000);
	}
}
