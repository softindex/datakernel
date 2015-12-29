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

package io.datakernel.eventloop.jmx;

import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.util.ExceptionMarker;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NioEventloopJmxStatsManager implements NioEventloopJmxStatsManagerMBean {

	private static final String EXCEPTION_COMPOSITE_DATE_NAME = "Exception Composite Data";
	private static final String EXCEPTION_MARKER_KEY = "Marker";
	public static final String LAST_EXCEPTION_KEY = "Last server exception";
	public static final String TOTAL_EXCEPTIONS_KEY = "Total exceptions";

	private volatile boolean monitoring;
	private volatile double smoothingWindow;
	private volatile double smoothingPrecision;
	private List<? extends NioEventloopJmx> eventloops;

	public NioEventloopJmxStatsManager(double smoothingWindow, double smoothingPrecision,
	                                   List<? extends NioEventloopJmx> eventloops) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.monitoring = false;
		this.eventloops = new ArrayList<>(eventloops);
		for (NioEventloopJmx eventloop : this.eventloops) {
			eventloop.resetStats(smoothingWindow, smoothingPrecision);
		}
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		for (NioEventloopJmx eventloop : eventloops) {
			eventloop.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (NioEventloopJmx eventloop : eventloops) {
			eventloop.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (NioEventloopJmx eventloop : eventloops) {
			eventloop.resetStats(smoothingWindow, smoothingPrecision);
		}
	}

	@Override
	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		for (NioEventloopJmx eventloop : eventloops) {
			eventloop.resetStats(smoothingWindow, smoothingPrecision);
		}

	}

	@Override
	public long getKeysStats_SelectedKeys() {
		return collectAllStats().getSelectedKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_SelectedKeysRate() {
		return collectAllStats().getSelectedKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_SelectedKeysDetails() {
		return collectAllStats().getSelectedKeys().toString();
	}

	@Override
	public long getKeysStats_InvalidKeys() {
		return collectAllStats().getInvalidKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_InvalidKeysRate() {
		return collectAllStats().getInvalidKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_InvalidKeysDetails() {
		return collectAllStats().getInvalidKeys().toString();
	}

	@Override
	public long getKeysStats_AcceptKeys() {
		return collectAllStats().getAcceptKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_AcceptKeysRate() {
		return collectAllStats().getAcceptKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_AcceptKeysDetails() {
		return collectAllStats().getAcceptKeys().toString();
	}

	@Override
	public long getKeysStats_ConnectKeys() {
		return collectAllStats().getConnectKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_ConnectKeysRate() {
		return collectAllStats().getConnectKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_ConnectKeysDetails() {
		return collectAllStats().getConnectKeys().toString();
	}

	@Override
	public long getKeysStats_ReadKeys() {
		return collectAllStats().getReadKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_ReadKeysRate() {
		return collectAllStats().getReadKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_ReadKeysDetails() {
		return collectAllStats().getReadKeys().toString();
	}

	@Override
	public long getKeysStats_WriteKeys() {
		return collectAllStats().getWriteKeys().getEventsCount();
	}

	@Override
	public double getKeysStats_WriteKeysRate() {
		return collectAllStats().getWriteKeys().getSmoothedRate();
	}

	@Override
	public String getKeysStats_WriteKeysDetails() {
		return collectAllStats().getWriteKeys().toString();
	}

	@Override
	public long getTaskStats_LocalTasks() {
		return collectAllStats().getLocalTasks().getEventsCount();
	}

	@Override
	public double getTaskStats_LocalTasksRate() {
		return collectAllStats().getLocalTasks().getSmoothedRate();
	}

	@Override
	public String getTaskStats_LocalTasksDetails() {
		return collectAllStats().getLocalTasks().toString();
	}

	@Override
	public long getTaskStats_ConcurrentTasks() {
		return collectAllStats().getConcurrentTasks().getEventsCount();
	}

	@Override
	public double getTaskStats_ConcurrentTasksRate() {
		return collectAllStats().getConcurrentTasks().getSmoothedRate();
	}

	@Override
	public String getTaskStats_ConcurrentTasksDetails() {
		return collectAllStats().getConcurrentTasks().toString();
	}

	@Override
	public long getTaskStats_ScheduledTasks() {
		return collectAllStats().getScheduledTasks().getEventsCount();
	}

	@Override
	public double getTaskStats_ScheduledTasksRate() {
		return collectAllStats().getScheduledTasks().getSmoothedRate();
	}

	@Override
	public String getTaskStats_ScheduledTasksDetails() {
		return collectAllStats().getScheduledTasks().toString();
	}

	@Override
	public double getTimeStats_SelectorSelect() {
		return collectAllStats().getSelectorSelectTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_SelectorSelectDetails() {
		return collectAllStats().getSelectorSelectTimeStats().toString();
	}

	@Override
	public double getTimeStats_BusinessLogic() {
		return collectAllStats().getBusinessLogicTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_BusinessLogicDetails() {
		return collectAllStats().getBusinessLogicTimeStats().toString();
	}

	@Override
	public double getTimeStats_SelectedKeys() {
		return collectAllStats().getSelectedKeysTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_SelectedKeysDetails() {
		return collectAllStats().getSelectedKeysTimeStats().toString();
	}

	@Override
	public double getTimeStats_LocalTasks() {
		return collectAllStats().getLocalTasksTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_LocalTasksDetails() {
		return collectAllStats().getLocalTasksTimeStats().toString();
	}

	@Override
	public double getTimeStats_ConcurrentTasks() {
		return collectAllStats().getConcurrentTasksTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_ConcurrentTasksDetails() {
		return collectAllStats().getConcurrentTasksTimeStats().toString();
	}

	@Override
	public double getTimeStats_ScheduledTasks() {
		return collectAllStats().getScheduledTasksTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_ScheduledTasksDetails() {
		return collectAllStats().getScheduledTasksTimeStats().toString();
	}

	@Override
	public double getDurationStats_LocalTasks() {
		return collectAllStats().getLocalTaskDuration().getSmoothedAverage();
	}

	@Override
	public String getDurationStats_LocalTasksDetails() {
		return collectAllStats().getLocalTaskDuration().toString();
	}

	@Override
	public double getDurationStats_ConcurrentTasks() {
		return collectAllStats().getConcurrentTaskDuration().getSmoothedAverage();
	}

	@Override
	public String getDurationStats_ConcurrentTasksDetails() {
		return collectAllStats().getConcurrentTaskDuration().toString();
	}

	@Override
	public double getDurationStats_ScheduledTasks() {
		return collectAllStats().getScheduledTaskDuration().getSmoothedAverage();
	}

	@Override
	public String getDurationStats_ScheduledTasksDetails() {
		return collectAllStats().getScheduledTasksTimeStats().toString();
	}

	@Override
	public CompositeData[] getExceptionStats_AllExceptions() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<ExceptionMarker, LastExceptionCounter.Accumulator> exceptionCounters = collectAllStats().getAllExceptionCounters();
		for (ExceptionMarker marker : exceptionCounters.keySet()) {
			LastExceptionCounter.Accumulator exceptionCounter = exceptionCounters.get(marker);
			Throwable lastException = exceptionCounter.getLastException();
			CompositeData compositeData = CompositeDataBuilder.builder(EXCEPTION_COMPOSITE_DATE_NAME)
					.add(EXCEPTION_MARKER_KEY, SimpleType.STRING, marker.getMarker().toString())
					.add(LAST_EXCEPTION_KEY, SimpleType.STRING,
							lastException != null ? lastException.toString() : "")
					.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
							Integer.toString(exceptionCounter.getTotalExceptions()))
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	private NioEventloopStatsSet.Accumulator collectAllStats() {
		NioEventloopStatsSet.Accumulator accumulator = NioEventloopStatsSet.accumulator();
		for (NioEventloopJmx eventloop : eventloops) {
			accumulator.add(eventloop.getStatsSet());
		}
		return accumulator;
	}

	@Override
	public int getExceptionStats_SevereExceptionsCount() {
		Map<Class<? extends Throwable>, LastExceptionCounter.Accumulator> severeExceptionCounters =
				collectAllStats().getSevereExceptionCounters();
		int severeExceptionsCount = 0;
		for (LastExceptionCounter.Accumulator accumulator : severeExceptionCounters.values()) {
			severeExceptionsCount += accumulator.getTotalExceptions();
		}
		return severeExceptionsCount;
	}

	@Override
	public CompositeData[] getExceptionStats_SevereExceptionsDetails() throws OpenDataException {
		Map<Class<? extends Throwable>, LastExceptionCounter.Accumulator> severeExceptionCounters =
				collectAllStats().getSevereExceptionCounters();
		List<CompositeData> compositeDataList = new ArrayList<>();
		for (Class<? extends Throwable> exceptionType : severeExceptionCounters.keySet()) {
			LastExceptionCounter.Accumulator exceptionCounter = severeExceptionCounters.get(exceptionType);
			CompositeData compositeData = exceptionCounter.compositeData();
			if (compositeData != null) {
				compositeDataList.add(compositeData);
			}
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}
}
