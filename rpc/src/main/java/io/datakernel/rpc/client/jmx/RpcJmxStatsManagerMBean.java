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

package io.datakernel.rpc.client.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

public interface RpcJmxStatsManagerMBean {

	void startMonitoring();

	void stopMonitoring();

	boolean isMonitoring();

	void resetStats();

	void resetStats(double smoothingWindow, double smoothingPrecision);

	// TODO(vmykhalko): is such functionality needed?
//	void resetStats(double shortTermWindow, double shortTermPrecision, double longTermWindow, double longTermPrecision);

	CompositeData[] getConnectionStats_Addresses() throws OpenDataException;

	int getConnectionStats_ActiveConnectionsCount();

	// grouped stats
	CompositeData[] getDistributedStats_AddressesStats() throws OpenDataException;

	CompositeData[] getDistributedStats_RequestClassesStats() throws OpenDataException;

	// request stats
	long getRequestStats_TotalRequests();

	double getRequestStats_TotalRequestsRate();

	String getRequestStats_TotalRequestsDetails();

	long getRequestStats_SuccessfulRequests();

	double getRequestStats_SuccessfulRequestsRate();

	String getRequestStats_SuccessfulRequestsDetails();

	long getRequestStats_FailedOnServerRequests();

	double getRequestStats_FailedOnServerRequestsRate();

	String getRequestStats_FailedOnServerRequestsDetails();

	long getRequestStats_RejectedRequests();

	double getRequestStats_RejectedRequestsRate();

	String getRequestStats_RejectedRequestsDetails();

	long getRequestStats_ExpiredRequests();

	double getRequestStats_ExpiredRequestsRate();

	String getRequestStats_ExpiredRequestsDetails();

	// connects stats
	int getConnectionStats_SuccessfulConnects();

	String getConnectionStats_SuccessfulConnectsDetails();

	int getConnectionStats_FailedConnects();

	String getConnectionStats_FailedConnectsDetails();

	int getConnectionStats_ClosedConnects();

	String getConnectionStats_ClosedConnectsDetails();

	// response time
	double getTimeStats_AverageResponseTime();

	String getTimeStats_AverageResponseTimeDetails();

	// exceptions
	String getExceptionStats_LastServerException();

	int getExceptionStats_ExceptionsCount();
}
