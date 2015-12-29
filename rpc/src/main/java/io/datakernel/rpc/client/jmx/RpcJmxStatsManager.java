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

import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.LastExceptionCounter;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread safe class
 */
public final class RpcJmxStatsManager implements RpcJmxStatsManagerMBean {

	// CompositeData keys
	public static final String REQUEST_CLASS_KEY = "_Request class";
	public static final String ADDRESS_KEY = "Address";
	public static final String TOTAL_REQUESTS_KEY = "Total requests";
	public static final String SUCCESSFUL_REQUESTS_KEY = "Successful requests";
	public static final String FAILED_REQUESTS_KEY = "Failed requests";
	public static final String REJECTED_REQUESTS_KEY = "Rejected requests";
	public static final String EXPIRED_REQUESTS_KEY = "Expired requests";
	public static final String RESPONSE_TIME_KEY = "Response time";
	public static final String LAST_SERVER_EXCEPTION_KEY = "Last server exception";
	public static final String TOTAL_EXCEPTIONS_KEY = "Total exceptions";
	public static final String SUCCESSFUL_CONNECTS_KEY = "Successful connects";
	public static final String FAILED_CONNECTS_KEY = "Failed connects";
	public static final String CLOSED_CONNECTS_KEY = "Closed connects";

	private static final String REQUEST_CLASS_COMPOSITE_DATA_NAME = "Request class stats";
	private static final String ADDRESS_COMPOSITE_DATA_NAME = "Address stats";

	// settings
	private volatile boolean monitoring;
	private volatile double smoothingWindow;
	private volatile double smoothingPrecision;
	private final List<RpcJmxClient> rpcClients;

	public RpcJmxStatsManager(double smoothingWindow, double smoothingPrecision, List<? extends RpcJmxClient> rpcClients) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.rpcClients = new ArrayList<>(rpcClients);
		for (RpcJmxClient rpcClient : this.rpcClients) {
			rpcClient.reset(smoothingWindow, smoothingPrecision);
		}
	}

	// jmx api
	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.reset(smoothingWindow, smoothingPrecision);
		}
	}

	@Override
	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.reset(smoothingWindow, smoothingPrecision);
		}
	}

	@Override
	public CompositeData[] getConnectionStats_Addresses() throws OpenDataException {
		List<InetSocketAddress> addresses = getClientsAddresses();

		List<CompositeData> compositeDataList = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			CompositeData compositeData = CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
					.add(ADDRESS_KEY, SimpleType.STRING, address.toString())
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public int getConnectionStats_ActiveConnectionsCount() {
		int totalConnectionsCount = 0;
		for (RpcJmxClient rpcClient : rpcClients) {
			totalConnectionsCount += rpcClient.getActiveConnectionsCount();
		}
		return totalConnectionsCount;
	}

	@Override
	public CompositeData[] getDistributedStats_AddressesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<InetSocketAddress, RpcJmxRequestsStatsSet.Accumulator> addressToGatheredRequestsStats =
				getGatheredRequestsStatsPerAddress();
		Map<InetSocketAddress, RpcJmxConnectsStatsSet.Accumulator> addressToGatheredConnectsStats =
				getGatheredConnectsStatsPerAddress();
		List<InetSocketAddress> addresses = getClientsAddresses();
		for (InetSocketAddress address : addresses) {
			CompositeDataBuilder.Builder builder = CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
					.add(ADDRESS_KEY, SimpleType.STRING, address.toString());

			RpcJmxRequestsStatsSet.Accumulator requestsStatsSet = addressToGatheredRequestsStats.get(address);
			RpcJmxConnectsStatsSet.Accumulator connectsStatsSet = addressToGatheredConnectsStats.get(address);

			if (requestsStatsSet != null) {
				LastExceptionCounter.Accumulator exeptionCounterAccumulator = requestsStatsSet.getLastServerException();
				Throwable lastException = exeptionCounterAccumulator.getLastException();
				builder = builder
						.add(TOTAL_REQUESTS_KEY, SimpleType.STRING,
								requestsStatsSet.getTotalRequests().toString())
						.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING,
								requestsStatsSet.getSuccessfulRequests().toString())
						.add(FAILED_REQUESTS_KEY, SimpleType.STRING,
								requestsStatsSet.getFailedRequests().toString())
						.add(REJECTED_REQUESTS_KEY, SimpleType.STRING,
								requestsStatsSet.getRejectedRequests().toString())
						.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING,
								requestsStatsSet.getExpiredRequests().toString())
						.add(RESPONSE_TIME_KEY, SimpleType.STRING,
								requestsStatsSet.getResponseTimeStats().toString())
						.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
								lastException != null ? lastException.toString() : "")
						.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
								Integer.toString(exeptionCounterAccumulator.getTotalExceptions()));
			}

			if (connectsStatsSet != null) {
				builder = builder
						.add(SUCCESSFUL_CONNECTS_KEY, SimpleType.STRING,
								connectsStatsSet.getSuccessfulConnects().toString())
						.add(FAILED_CONNECTS_KEY, SimpleType.STRING,
								connectsStatsSet.getFailedConnects().toString())
						.add(CLOSED_CONNECTS_KEY, SimpleType.STRING,
								connectsStatsSet.getClosedConnects().toString());
			}

			compositeDataList.add(builder.build());
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public CompositeData[] getDistributedStats_RequestClassesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<Class<?>, RpcJmxRequestsStatsSet.Accumulator> classToGatheredStats = getGatheredStatsPerClass();
		for (Class<?> requestClass : classToGatheredStats.keySet()) {
			RpcJmxRequestsStatsSet.Accumulator requestStatsSet = classToGatheredStats.get(requestClass);
			LastExceptionCounter.Accumulator lastExceptionAccumulator = requestStatsSet.getLastServerException();
			Throwable lastException = lastExceptionAccumulator.getLastException();
			CompositeData compositeData = CompositeDataBuilder.builder(REQUEST_CLASS_COMPOSITE_DATA_NAME)
					.add(REQUEST_CLASS_KEY, SimpleType.STRING,
							requestClass.getName())
					.add(TOTAL_REQUESTS_KEY, SimpleType.STRING,
							requestStatsSet.getTotalRequests().toString())
					.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING,
							requestStatsSet.getSuccessfulRequests().toString())
					.add(FAILED_REQUESTS_KEY, SimpleType.STRING,
							requestStatsSet.getFailedRequests().toString())
					.add(REJECTED_REQUESTS_KEY, SimpleType.STRING,
							requestStatsSet.getRejectedRequests().toString())
					.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING,
							requestStatsSet.getExpiredRequests().toString())
					.add(RESPONSE_TIME_KEY, SimpleType.STRING,
							requestStatsSet.getResponseTimeStats().toString())
					.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
							lastException != null ? lastException.toString() : "")
					.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
							Integer.toString(lastExceptionAccumulator.getTotalExceptions()))
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);

	}

	@Override
	public long getRequestStats_TotalRequests() {
		return collectGeneralRequestsStatsFromAllClients().getTotalRequests().getEventsCount();
	}

	@Override
	public double getRequestStats_TotalRequestsRate() {
		return collectGeneralRequestsStatsFromAllClients().getTotalRequests().getSmoothedRate();
	}

	@Override
	public String getRequestStats_TotalRequestsDetails() {
		return collectGeneralRequestsStatsFromAllClients().getTotalRequests().toString();
	}

	@Override
	public long getRequestStats_SuccessfulRequests() {
		return collectGeneralRequestsStatsFromAllClients().getSuccessfulRequests().getEventsCount();
	}

	@Override
	public double getRequestStats_SuccessfulRequestsRate() {
		return collectGeneralRequestsStatsFromAllClients().getSuccessfulRequests().getSmoothedRate();
	}

	@Override
	public String getRequestStats_SuccessfulRequestsDetails() {
		return collectGeneralRequestsStatsFromAllClients().getSuccessfulRequests().toString();
	}

	@Override
	public long getRequestStats_FailedOnServerRequests() {
		return collectGeneralRequestsStatsFromAllClients().getFailedRequests().getEventsCount();
	}

	@Override
	public double getRequestStats_FailedOnServerRequestsRate() {
		return collectGeneralRequestsStatsFromAllClients().getFailedRequests().getSmoothedRate();
	}

	@Override
	public String getRequestStats_FailedOnServerRequestsDetails() {
		return collectGeneralRequestsStatsFromAllClients().getFailedRequests().toString();
	}

	@Override
	public long getRequestStats_RejectedRequests() {
		return collectGeneralRequestsStatsFromAllClients().getRejectedRequests().getEventsCount();
	}

	@Override
	public double getRequestStats_RejectedRequestsRate() {
		return collectGeneralRequestsStatsFromAllClients().getRejectedRequests().getSmoothedRate();
	}

	@Override
	public String getRequestStats_RejectedRequestsDetails() {
		return collectGeneralRequestsStatsFromAllClients().getRejectedRequests().toString();
	}

	@Override
	public long getRequestStats_ExpiredRequests() {
		return collectGeneralRequestsStatsFromAllClients().getExpiredRequests().getEventsCount();
	}

	@Override
	public double getRequestStats_ExpiredRequestsRate() {
		return collectGeneralRequestsStatsFromAllClients().getExpiredRequests().getSmoothedRate();
	}

	@Override
	public String getRequestStats_ExpiredRequestsDetails() {
		return collectGeneralRequestsStatsFromAllClients().getExpiredRequests().toString();
	}

	@Override
	public int getConnectionStats_SuccessfulConnects() {
		return (int) collectGeneralConnectsStatsSetsFromAllClients().getSuccessfulConnects().getEventsCount();
	}

	@Override
	public String getConnectionStats_SuccessfulConnectsDetails() {
		return collectGeneralConnectsStatsSetsFromAllClients().getSuccessfulConnects().toString();
	}

	@Override
	public int getConnectionStats_FailedConnects() {
		return (int) collectGeneralConnectsStatsSetsFromAllClients().getFailedConnects().getEventsCount();
	}

	@Override
	public String getConnectionStats_FailedConnectsDetails() {
		return collectGeneralConnectsStatsSetsFromAllClients().getFailedConnects().toString();
	}

	@Override
	public int getConnectionStats_ClosedConnects() {
		return (int) collectGeneralConnectsStatsSetsFromAllClients().getClosedConnects().getEventsCount();
	}

	@Override
	public String getConnectionStats_ClosedConnectsDetails() {
		return collectGeneralConnectsStatsSetsFromAllClients().getClosedConnects().toString();
	}

	@Override
	public double getTimeStats_AverageResponseTime() {
		return collectGeneralRequestsStatsFromAllClients().getResponseTimeStats().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_AverageResponseTimeDetails() {
		return collectGeneralRequestsStatsFromAllClients().getResponseTimeStats().toString();
	}

	@Override
	public String getExceptionStats_LastServerException() {
		Throwable lastException =
				collectGeneralRequestsStatsFromAllClients().getLastServerException().getLastException();
		return lastException != null ? lastException.toString() : "";
	}

	@Override
	public int getExceptionStats_ExceptionsCount() {
		return collectGeneralRequestsStatsFromAllClients().getLastServerException().getTotalExceptions();
	}

	// methods to simplify collecting stats from rpcClients
	private RpcJmxRequestsStatsSet.Accumulator collectGeneralRequestsStatsFromAllClients() {
		RpcJmxRequestsStatsSet.Accumulator accumulator = RpcJmxRequestsStatsSet.accumulator();
		for (RpcJmxClient rpcClient : rpcClients) {
			accumulator.add(rpcClient.getGeneralRequestsStats());
		}
		return accumulator;
	}

	private RpcJmxConnectsStatsSet.Accumulator collectGeneralConnectsStatsSetsFromAllClients() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsStats =
				collectConnectsStatsPerAddressFromAllClients();
		RpcJmxConnectsStatsSet.Accumulator accumulator = RpcJmxConnectsStatsSet.accumulator();
		for (Map<InetSocketAddress, RpcJmxConnectsStatsSet> clientConnectsStats : clientsStats) {
			for (RpcJmxConnectsStatsSet statsSet : clientConnectsStats.values()) {
				accumulator.add(statsSet);
			}
		}
		return accumulator;
	}

	private List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> collectConnectsStatsPerAddressFromAllClients() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsConnectsStats = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsConnectsStats.add(rpcClient.getConnectsStatsPerAddress());
		}
		return clientsConnectsStats;
	}

	private List<Map<Class<?>, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerClass() {
		List<Map<Class<?>, RpcJmxRequestsStatsSet>> clientsStatsPerClass = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsStatsPerClass.add(rpcClient.getRequestsStatsPerClass());
		}
		return clientsStatsPerClass;
	}

	private List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> clientsAddressesStats = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsAddressesStats.add(rpcClient.getRequestStatsPerAddress());
		}
		return clientsAddressesStats;
	}

	private List<InetSocketAddress> getClientsAddresses() {
		List<InetSocketAddress> allClientsAddresses = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			for (InetSocketAddress address : rpcClient.getAddresses()) {
				if (!allClientsAddresses.contains(address)) {
					allClientsAddresses.add(address);
				}
			}
		}
		return allClientsAddresses;
	}

	// methods for regrouping / reducing / gathering
	private Map<Class<?>, RpcJmxRequestsStatsSet.Accumulator> getGatheredStatsPerClass() {
		List<Map<Class<?>, RpcJmxRequestsStatsSet>> allClientsStats = getClientsRequestsStatsPerClass();
		Map<Class<?>, RpcJmxRequestsStatsSet.Accumulator> classToGatheredStatsList = new HashMap<>();
		for (Map<Class<?>, RpcJmxRequestsStatsSet> singleClientStatsPerClass : allClientsStats) {
			for (Class<?> requestClass : singleClientStatsPerClass.keySet()) {
				if (!classToGatheredStatsList.containsKey(requestClass)) {
					classToGatheredStatsList.put(requestClass, RpcJmxRequestsStatsSet.accumulator());
				}
				RpcJmxRequestsStatsSet.Accumulator accumulator = classToGatheredStatsList.get(requestClass);
				RpcJmxRequestsStatsSet currentStats = singleClientStatsPerClass.get(requestClass);
				accumulator.add(currentStats);
			}
		}
		return classToGatheredStatsList;
	}

	private Map<InetSocketAddress, RpcJmxRequestsStatsSet.Accumulator> getGatheredRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> allClientsRequestsStats = getClientsRequestsStatsPerAddress();
		Map<InetSocketAddress, RpcJmxRequestsStatsSet.Accumulator> addressToGatheredRequestsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcJmxRequestsStatsSet> singleClientRequestsStatsPerAddress : allClientsRequestsStats) {
			for (InetSocketAddress address : singleClientRequestsStatsPerAddress.keySet()) {
				if (!addressToGatheredRequestsStatsList.containsKey(address)) {
					addressToGatheredRequestsStatsList.put(address, RpcJmxRequestsStatsSet.accumulator());
				}
				RpcJmxRequestsStatsSet.Accumulator accumulator = addressToGatheredRequestsStatsList.get(address);
				RpcJmxRequestsStatsSet currentRequestsStats = singleClientRequestsStatsPerAddress.get(address);
				accumulator.add(currentRequestsStats);
			}
		}
		return addressToGatheredRequestsStatsList;
	}

	private Map<InetSocketAddress, RpcJmxConnectsStatsSet.Accumulator> getGatheredConnectsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> allClientsConnectsStats =
				collectConnectsStatsPerAddressFromAllClients();
		Map<InetSocketAddress, RpcJmxConnectsStatsSet.Accumulator> addressToGatheredConnectsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcJmxConnectsStatsSet> singleClientConnectsStatsPerAddress : allClientsConnectsStats) {
			for (InetSocketAddress address : singleClientConnectsStatsPerAddress.keySet()) {
				if (!addressToGatheredConnectsStatsList.containsKey(address)) {
					addressToGatheredConnectsStatsList.put(address, RpcJmxConnectsStatsSet.accumulator());
				}
				RpcJmxConnectsStatsSet.Accumulator accumulator = addressToGatheredConnectsStatsList.get(address);
				RpcJmxConnectsStatsSet currentRequestsStats = singleClientConnectsStatsPerAddress.get(address);
				accumulator.add(currentRequestsStats);
			}
		}
		return addressToGatheredConnectsStatsList;
	}
}