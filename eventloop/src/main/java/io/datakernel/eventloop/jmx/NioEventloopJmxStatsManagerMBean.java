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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

public interface NioEventloopJmxStatsManagerMBean {

	void startMonitoring();

	void stopMonitoring();

	boolean isMonitoring();

	void resetStats();

	void resetStats(double smoothingWindow, double smoothingPrecision);

	// keys stats
	long getKeysStats_SelectedKeys();

	double getKeysStats_SelectedKeysRate();

	String getKeysStats_SelectedKeysDetails();

	long getKeysStats_InvalidKeys();

	double getKeysStats_InvalidKeysRate();

	String getKeysStats_InvalidKeysDetails();

	long getKeysStats_AcceptKeys();

	double getKeysStats_AcceptKeysRate();

	String getKeysStats_AcceptKeysDetails();

	long getKeysStats_ConnectKeys();

	double getKeysStats_ConnectKeysRate();

	String getKeysStats_ConnectKeysDetails();

	long getKeysStats_ReadKeys();

	double getKeysStats_ReadKeysRate();

	String getKeysStats_ReadKeysDetails();

	long getKeysStats_WriteKeys();

	double getKeysStats_WriteKeysRate();

	String getKeysStats_WriteKeysDetails();

	// task stats
	long getTaskStats_LocalTasks();

	double getTaskStats_LocalTasksRate();

	String getTaskStats_LocalTasksDetails();

	long getTaskStats_ConcurrentTasks();

	double getTaskStats_ConcurrentTasksRate();

	String getTaskStats_ConcurrentTasksDetails();

	long getTaskStats_ScheduledTasks();

	double getTaskStats_ScheduledTasksRate();

	String getTaskStats_ScheduledTasksDetails();

	// time stats
	double getTimeStats_SelectorSelect();

	String getTimeStats_SelectorSelectDetails();

	double getTimeStats_BusinessLogic();

	String getTimeStats_BusinessLogicDetails();

	double getTimeStats_SelectedKeys();

	String getTimeStats_SelectedKeysDetails();

	double getTimeStats_LocalTasks();

	String getTimeStats_LocalTasksDetails();

	double getTimeStats_ConcurrentTasks();

	String getTimeStats_ConcurrentTasksDetails();

	double getTimeStats_ScheduledTasks();

	String getTimeStats_ScheduledTasksDetails();

	// duration stats
	double getDurationStats_LocalTasks();

	String getDurationStats_LocalTasksDetails();

	double getDurationStats_ConcurrentTasks();

	String getDurationStats_ConcurrentTasksDetails();

	double getDurationStats_ScheduledTasks();

	String getDurationStats_ScheduledTasksDetails();

	// exception stats
	CompositeData[] getExceptionStats_AllExceptions() throws OpenDataException;

	int getExceptionStats_SevereExceptionsCount();

	CompositeData[] getExceptionStats_SevereExceptionsDetails() throws OpenDataException;

}
