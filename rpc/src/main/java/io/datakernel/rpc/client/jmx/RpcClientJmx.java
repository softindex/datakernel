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
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.rpc.client.RpcClient;

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
public final class RpcClientJmx implements RpcClientJmxMBean {

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
	private final List<RpcClient> rpcClients;

	public RpcClientJmx(List<? extends RpcClient> rpcClients) {
		this.rpcClients = new ArrayList<>(rpcClients);
	}

	// jmx api
	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcClient rpcClient : rpcClients) {
			rpcClient.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcClient rpcClient : rpcClients) {
			rpcClient.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (RpcClient rpcClient : rpcClients) {
			rpcClient.resetStats();
		}
	}

	@Override
	public void setSmoothingWindow(double smoothingWindow) {
		for (RpcClient rpcClient : rpcClients) {
			rpcClient.setSmoothingWindow(smoothingWindow);
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
		for (RpcClient rpcClient : rpcClients) {
			totalConnectionsCount += rpcClient.getActiveConnectionsCount();
		}
		return totalConnectionsCount;
	}

	@Override
	public CompositeData[] getDistributedStats_AddressesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<InetSocketAddress, RpcRequestsStats> addressToGatheredRequestsStats =
				getGatheredRequestsStatsPerAddress();
		Map<InetSocketAddress, RpcConnectsStats> addressToGatheredConnectsStats =
				getGatheredConnectsStatsPerAddress();
		List<InetSocketAddress> addresses = getClientsAddresses();
		for (InetSocketAddress address : addresses) {
			CompositeDataBuilder.Builder builder = CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
					.add(ADDRESS_KEY, SimpleType.STRING, address.toString());

			RpcRequestsStats requestsStatsSet = addressToGatheredRequestsStats.get(address);
			RpcConnectsStats connectsStatsSet = addressToGatheredConnectsStats.get(address);

			if (requestsStatsSet != null) {
				ExceptionStats exceptionStats = requestsStatsSet.getServerExceptions();
				Throwable lastException = exceptionStats.getLastException();
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
								requestsStatsSet.getResponseTime().toString())
						.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
								lastException != null ? lastException.toString() : "")
						.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
								Integer.toString(exceptionStats.getCount()));
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
		Map<Class<?>, RpcRequestsStats> classToGatheredStats = getGatheredStatsPerClass();
		for (Class<?> requestClass : classToGatheredStats.keySet()) {
			RpcRequestsStats requestStats = classToGatheredStats.get(requestClass);
			ExceptionStats exceptionStats = requestStats.getServerExceptions();
			Throwable lastException = exceptionStats.getLastException();
			CompositeData compositeData = CompositeDataBuilder.builder(REQUEST_CLASS_COMPOSITE_DATA_NAME)
					.add(REQUEST_CLASS_KEY, SimpleType.STRING,
							requestClass.getName())
					.add(TOTAL_REQUESTS_KEY, SimpleType.STRING,
							requestStats.getTotalRequests().toString())
					.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING,
							requestStats.getSuccessfulRequests().toString())
					.add(FAILED_REQUESTS_KEY, SimpleType.STRING,
							requestStats.getFailedRequests().toString())
					.add(REJECTED_REQUESTS_KEY, SimpleType.STRING,
							requestStats.getRejectedRequests().toString())
					.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING,
							requestStats.getExpiredRequests().toString())
					.add(RESPONSE_TIME_KEY, SimpleType.STRING,
							requestStats.getResponseTime().toString())
					.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
							lastException != null ? lastException.toString() : "")
					.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
							Integer.toString(exceptionStats.getCount()))
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);

	}

	@Override
	public long getRequestStats_TotalRequests() {
		return collectGeneralRequestsStatsFromAllClients().getTotalRequests().getTotalCount();
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
		return collectGeneralRequestsStatsFromAllClients().getSuccessfulRequests().getTotalCount();
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
		return collectGeneralRequestsStatsFromAllClients().getFailedRequests().getTotalCount();
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
		return collectGeneralRequestsStatsFromAllClients().getRejectedRequests().getTotalCount();
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
		return collectGeneralRequestsStatsFromAllClients().getExpiredRequests().getTotalCount();
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
		return (int) collectGeneralConnectsStatsFromAllClients().getSuccessfulConnects().getTotalCount();
	}

	@Override
	public String getConnectionStats_SuccessfulConnectsDetails() {
		return collectGeneralConnectsStatsFromAllClients().getSuccessfulConnects().toString();
	}

	@Override
	public int getConnectionStats_FailedConnects() {
		return (int) collectGeneralConnectsStatsFromAllClients().getFailedConnects().getTotalCount();
	}

	@Override
	public String getConnectionStats_FailedConnectsDetails() {
		return collectGeneralConnectsStatsFromAllClients().getFailedConnects().toString();
	}

	@Override
	public int getConnectionStats_ClosedConnects() {
		return (int) collectGeneralConnectsStatsFromAllClients().getClosedConnects().getTotalCount();
	}

	@Override
	public String getConnectionStats_ClosedConnectsDetails() {
		return collectGeneralConnectsStatsFromAllClients().getClosedConnects().toString();
	}

	@Override
	public double getTimeStats_AverageResponseTime() {
		return collectGeneralRequestsStatsFromAllClients().getResponseTime().getSmoothedAverage();
	}

	@Override
	public String getTimeStats_AverageResponseTimeDetails() {
		return collectGeneralRequestsStatsFromAllClients().getResponseTime().toString();
	}

	@Override
	public String getExceptionStats_LastServerException() {
		Throwable lastException =
				collectGeneralRequestsStatsFromAllClients().getServerExceptions().getLastException();
		return lastException != null ? lastException.toString() : "";
	}

	@Override
	public int getExceptionStats_ExceptionsCount() {
		return collectGeneralRequestsStatsFromAllClients().getServerExceptions().getCount();
	}

	// methods to simplify collecting stats from rpcClients
	private RpcRequestsStats collectGeneralRequestsStatsFromAllClients() {
		RpcRequestsStats accumulator = new RpcRequestsStats();
		for (RpcClient rpcClient : rpcClients) {
			accumulator.add(rpcClient.getGeneralRequestsStats());
		}
		return accumulator;
	}

	private RpcConnectsStats collectGeneralConnectsStatsFromAllClients() {
		List<Map<InetSocketAddress, RpcConnectsStats>> clientsStats =
				collectConnectsStatsPerAddressFromAllClients();
		RpcConnectsStats accumulator = new RpcConnectsStats();
		for (Map<InetSocketAddress, RpcConnectsStats> clientConnectsStats : clientsStats) {
			for (RpcConnectsStats statsSet : clientConnectsStats.values()) {
				accumulator.add(statsSet);
			}
		}
		return accumulator;
	}

	private List<Map<InetSocketAddress, RpcConnectsStats>> collectConnectsStatsPerAddressFromAllClients() {
		List<Map<InetSocketAddress, RpcConnectsStats>> clientsConnectsStats = new ArrayList<>();
		for (RpcClient rpcClient : rpcClients) {
			clientsConnectsStats.add(rpcClient.getConnectsStatsPerAddress());
		}
		return clientsConnectsStats;
	}

	private List<Map<Class<?>, RpcRequestsStats>> getClientsRequestsStatsPerClass() {
		List<Map<Class<?>, RpcRequestsStats>> clientsStatsPerClass = new ArrayList<>();
		for (RpcClient rpcClient : rpcClients) {
			clientsStatsPerClass.add(rpcClient.getRequestsStatsPerClass());
		}
		return clientsStatsPerClass;
	}

	private List<Map<InetSocketAddress, RpcRequestsStats>> getClientsRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcRequestsStats>> clientsAddressesStats = new ArrayList<>();
		for (RpcClient rpcClient : rpcClients) {
			clientsAddressesStats.add(rpcClient.getRequestStatsPerAddress());
		}
		return clientsAddressesStats;
	}

	private List<InetSocketAddress> getClientsAddresses() {
		List<InetSocketAddress> allClientsAddresses = new ArrayList<>();
		for (RpcClient rpcClient : rpcClients) {
			for (InetSocketAddress address : rpcClient.getAddresses()) {
				if (!allClientsAddresses.contains(address)) {
					allClientsAddresses.add(address);
				}
			}
		}
		return allClientsAddresses;
	}

	// methods for regrouping / reducing / gathering
	private Map<Class<?>, RpcRequestsStats> getGatheredStatsPerClass() {
		List<Map<Class<?>, RpcRequestsStats>> allClientsStats = getClientsRequestsStatsPerClass();
		Map<Class<?>, RpcRequestsStats> classToGatheredStatsList = new HashMap<>();
		for (Map<Class<?>, RpcRequestsStats> singleClientStatsPerClass : allClientsStats) {
			for (Class<?> requestClass : singleClientStatsPerClass.keySet()) {
				if (!classToGatheredStatsList.containsKey(requestClass)) {
					classToGatheredStatsList.put(requestClass, new RpcRequestsStats());
				}
				RpcRequestsStats accumulator = classToGatheredStatsList.get(requestClass);
				RpcRequestsStats currentStats = singleClientStatsPerClass.get(requestClass);
				accumulator.add(currentStats);
			}
		}
		return classToGatheredStatsList;
	}

	private Map<InetSocketAddress, RpcRequestsStats> getGatheredRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcRequestsStats>> allClientsRequestsStats = getClientsRequestsStatsPerAddress();
		Map<InetSocketAddress, RpcRequestsStats> addressToGatheredRequestsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcRequestsStats> singleClientRequestsStatsPerAddress : allClientsRequestsStats) {
			for (InetSocketAddress address : singleClientRequestsStatsPerAddress.keySet()) {
				if (!addressToGatheredRequestsStatsList.containsKey(address)) {
					addressToGatheredRequestsStatsList.put(address, new RpcRequestsStats());
				}
				RpcRequestsStats accumulator = addressToGatheredRequestsStatsList.get(address);
				RpcRequestsStats currentRequestsStats = singleClientRequestsStatsPerAddress.get(address);
				accumulator.add(currentRequestsStats);
			}
		}
		return addressToGatheredRequestsStatsList;
	}

	private Map<InetSocketAddress, RpcConnectsStats> getGatheredConnectsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcConnectsStats>> allClientsConnectsStats =
				collectConnectsStatsPerAddressFromAllClients();
		Map<InetSocketAddress, RpcConnectsStats> addressToGatheredConnectsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcConnectsStats> singleClientConnectsStatsPerAddress : allClientsConnectsStats) {
			for (InetSocketAddress address : singleClientConnectsStatsPerAddress.keySet()) {
				if (!addressToGatheredConnectsStatsList.containsKey(address)) {
					addressToGatheredConnectsStatsList.put(address, new RpcConnectsStats());
				}
				RpcConnectsStats accumulator = addressToGatheredConnectsStatsList.get(address);
				RpcConnectsStats currentRequestsStats = singleClientConnectsStatsPerAddress.get(address);
				accumulator.add(currentRequestsStats);
			}
		}
		return addressToGatheredConnectsStatsList;
	}
}