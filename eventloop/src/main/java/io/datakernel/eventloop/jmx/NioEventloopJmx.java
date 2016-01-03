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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.util.ExceptionMarker;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class NioEventloopJmx implements NioEventloopJmxMBean {

	private static final String EXCEPTION_COMPOSITE_DATE_NAME = "Exception Composite Data";
	private static final String EXCEPTION_MARKER_KEY = "Marker";
	public static final String LAST_EXCEPTION_KEY = "Last server exception";
	public static final String TOTAL_EXCEPTIONS_KEY = "Total exceptions";

	private volatile boolean monitoring;
	private List<NioEventloop> eventloops;

	public NioEventloopJmx(List<NioEventloop> eventloops) {
		this.monitoring = false;
		this.eventloops = new ArrayList<>(eventloops);
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		for (NioEventloop eventloop : eventloops) {
			eventloop.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (NioEventloop eventloop : eventloops) {
			eventloop.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (NioEventloop eventloop : eventloops) {
			eventloop.resetStats();
		}
	}

	@Override
	public void setSmoothingWindow(double smoothingWindow) {
		for (NioEventloop eventloop : eventloops) {
			eventloop.setSmoothingWindow(smoothingWindow);
		}

	}

	@Override
	public long getKeysStats_SelectedKeys() {
		return collectAllStats().getSelectedKeys().getTotalCount();
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
		return collectAllStats().getInvalidKeys().getTotalCount();
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
		return collectAllStats().getAcceptKeys().getTotalCount();
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
		return collectAllStats().getConnectKeys().getTotalCount();
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
		return collectAllStats().getReadKeys().getTotalCount();
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
		return collectAllStats().getWriteKeys().getTotalCount();
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
		return collectAllStats().getLocalTasks().getTotalCount();
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
		return collectAllStats().getConcurrentTasks().getTotalCount();
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
		return collectAllStats().getScheduledTasks().getTotalCount();
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
		return collectAllStats().getSelectorSelectTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_SelectorSelectDetails() {
		return collectAllStats().getSelectorSelectTime().toString();
	}

	@Override
	public double getTimeStats_BusinessLogic() {
		return collectAllStats().getBusinessLogicTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_BusinessLogicDetails() {
		return collectAllStats().getBusinessLogicTime().toString();
	}

	@Override
	public double getTimeStats_SelectedKeys() {
		return collectAllStats().getSelectedKeysTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_SelectedKeysDetails() {
		return collectAllStats().getSelectedKeysTime().toString();
	}

	@Override
	public double getTimeStats_LocalTasks() {
		return collectAllStats().getLocalTasksTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_LocalTasksDetails() {
		return collectAllStats().getLocalTasksTime().toString();
	}

	@Override
	public double getTimeStats_ConcurrentTasks() {
		return collectAllStats().getConcurrentTasksTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_ConcurrentTasksDetails() {
		return collectAllStats().getConcurrentTasksTime().toString();
	}

	@Override
	public double getTimeStats_ScheduledTasks() {
		return collectAllStats().getScheduledTasksTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_ScheduledTasksDetails() {
		return collectAllStats().getScheduledTasksTime().toString();
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
		return collectAllStats().getScheduledTasksTime().toString();
	}

	@Override
	public CompositeData[] getExceptionStats_AllExceptions() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<ExceptionMarker, ExceptionStats> exceptions = collectAllStats().getExceptions();
		for (ExceptionMarker marker : exceptions.keySet()) {
			ExceptionStats stats = exceptions.get(marker);
			Throwable lastException = stats.getLastException();
			CompositeData compositeData = CompositeDataBuilder.builder(EXCEPTION_COMPOSITE_DATE_NAME)
					.add(EXCEPTION_MARKER_KEY, SimpleType.STRING, marker.getMarker().toString())
					.add(LAST_EXCEPTION_KEY, SimpleType.STRING,
							lastException != null ? lastException.toString() : "")
					.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
							Integer.toString(stats.getCount())) // TODO (vmykhalko): check this
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	private NioEventloopStats collectAllStats() {
		NioEventloopStats stats = new NioEventloopStats();
		for (NioEventloop eventloop : eventloops) {
			stats.add(eventloop.getStats());
		}
		return stats;
	}

	@Override
	public int getExceptionStats_SevereExceptionsCount() {
		Map<Class<? extends Throwable>, ExceptionStats> severeExceptions = collectAllStats().getSevereExceptions();
		int severeExceptionsCount = 0;
		for (ExceptionStats stats : severeExceptions.values()) {
			severeExceptionsCount += stats.getCount();
		}
		return severeExceptionsCount;
	}

	@Override
	public CompositeData[] getExceptionStats_SevereExceptionsDetails() throws OpenDataException {
		Map<Class<? extends Throwable>, ExceptionStats> severeExceptions = collectAllStats().getSevereExceptions();
		List<CompositeData> compositeDataList = new ArrayList<>();
		for (Class<? extends Throwable> exceptionType : severeExceptions.keySet()) {
			ExceptionStats stats = severeExceptions.get(exceptionType);
			CompositeData compositeData = stats.compositeData();
			if (compositeData != null) {
				compositeDataList.add(compositeData);
			}
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}
}
