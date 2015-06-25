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

package io.datakernel.eventloop;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

public interface NioEventloopStatsMBean {
	void resetStats();

	void startMonitoring();

	void stopMonitoring();

	boolean isMonitoring();

	long getMonitoringLoop();

	String getMonitoringTime();

	int getSelectedKeys();

	long getInvalidKeys();

	long getAcceptKeys();

	long getConnectKeys();

	long getReadKeys();

	long getWriteKeys();

	String getSelectedKeysStats();

	String getInvalidKeysStats();

	String getAcceptKeysStats();

	String getConnectKeysStats();

	String getReadKeysStats();

	String getWriteKeysStats();

	int getConcurrentTasksPerLoop();

	String getConcurrentTasksPerLoopStats();

	int getLocalTasksPerLoop();

	String getLocalTasksPerLoopStats();

	int getScheduledTasksPerLoop();

	String getScheduledTasksPerLoopStats();

	long getBusinessLogicMillis();

	String getBusinessLogicMillisStats();

	long getSelectorSelectMillis();

	String getSelectorSelectMillisStats();

	CompositeData[] getLastExceptions() throws OpenDataException;

	int getConcurrentTaskMicros();

	String getConcurrentTaskStats();

	int getLocalTaskMicros();

	String getLocalTaskStats();

	int getScheduledTaskMicros();

	String getScheduledTaskStats();

	String getConcurrentTaskLongestMicros();

	String getLocalTaskLongestMicros();

	String getScheduledTaskLongestMicros();

	int getConcurrentTasksMillis();

	String getConcurrentTasksStats();

	int getLocalTasksMillis();

	String getLocalTasksStats();

	int getScheduledTasksMillis();

	String getScheduledTasksStats();

	int getSelectedKeysMillis();

	String getSelectedKeysMillisStats();

	long getLongLoopMillis();

	void setLongLoopMillis(long longLoopMillis);

	String getLongLoopsRate();

	String getLongLoopLocalTaskLongest();

	long getLongLoopConcurrentTasks();

	String getLongLoopConcurrentTasksStats();

	long getLongLoopLocalTasks();

	String getLongLoopLocalTasksStats();

	String getLongLoopConcurrentTaskLongest();

	long getLongLoopScheduledTasks();

	String getLongLoopScheduledTasksStats();

	String getLongLoopScheduledTaskLongest();

	CompositeData getLastLongLoopStats() throws OpenDataException;
}
