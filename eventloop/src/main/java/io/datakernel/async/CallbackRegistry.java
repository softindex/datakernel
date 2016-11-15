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

package io.datakernel.async;

import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.jmx.MBeanFormat.formatDuration;

public final class CallbackRegistry {
	private static final ConcurrentHashMap<Object, CallbackMetaData> REGISTRY = new ConcurrentHashMap<>();

	private static volatile boolean storeStackTrace = false;

	// region public api
	public static void register(Object callback) {
		assert registerIfAssertionsEnabled(callback) : "Callback " + callback + " has already been registered";

	}

	public static void complete(Object callback) {
		assert REGISTRY.remove(callback) != null : "Callback " + callback + " has already been completed";
	}

	public static boolean getStoreStackTrace() {
		return storeStackTrace;
	}

	public static void setStoreStackTrace(boolean store) {
		storeStackTrace = store;
	}
	// endregion

	// region implementation
	private static boolean registerIfAssertionsEnabled(Object callback) {
		long timestamp = System.currentTimeMillis();

		StackTraceElement[] stackTrace = null;
		if (storeStackTrace) {
			// TODO(vmykhalko): maybe use new Exception().getStackTrace instead ? according to performance issues
			StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
			// remove stack trace lines that stand for registration method calls
			stackTrace = Arrays.copyOfRange(fullStackTrace, 3, fullStackTrace.length);
		}

		return REGISTRY.put(callback, new CallbackMetaData(timestamp, stackTrace)) == null;
	}

	private static final class CallbackMetaData {
		private final long timestamp;
		private final StackTraceElement[] stackTrace;

		public CallbackMetaData(long timestamp, StackTraceElement[] stackTrace) {
			this.timestamp = timestamp;
			this.stackTrace = stackTrace;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public StackTraceElement[] getStackTrace() {
			return stackTrace;
		}
	}
	// endregion

	// region jmx
	public static final class CallbackRegistryStats implements ConcurrentJmxMBean {
		private volatile int maxCallbacksToShow = 100;

		@JmxAttribute
		public List<String> getCurrentCallbacksSummary() {
			List<CallbackDetails> callbacksDetails = getCurrentCallbacksDetails();

			List<String> lines = new ArrayList<>();
			lines.add("Duration       Callback");
			for (CallbackDetails details : callbacksDetails) {
				lines.add(String.format("%s   %s: %s",
						details.getDuration(), details.getClassName(), details.getStringRepresentation()));
			}

			return lines;
		}

		@JmxAttribute
		public List<CallbackDetails> getCurrentCallbacksDetails() {
			List<Map.Entry<Object, CallbackMetaData>> sortedEntries = sortByTimestamp(REGISTRY);

			List<CallbackDetails> detailsList = new ArrayList<>();
			long currentTime = System.currentTimeMillis();
			int maxCallbacksToShowCached = maxCallbacksToShow;
			for (Map.Entry<Object, CallbackMetaData> entry : sortedEntries) {
				if (detailsList.size() == maxCallbacksToShowCached) {
					break;
				}

				Object callback = entry.getKey();
				CallbackMetaData metaData = entry.getValue();
				String formattedDuration = formatDuration(currentTime - metaData.getTimestamp());
				String className = callback.getClass().getName();
				String callbackString = callback.toString();

				List<String> stackTraceLines = new ArrayList<>();
				StackTraceElement[] stackTrace = metaData.getStackTrace();
				if (stackTrace != null) {
					for (StackTraceElement stackTraceElement : stackTrace) {
						stackTraceLines.add(stackTraceElement.toString());
					}
				}

				detailsList.add(new CallbackDetails(className, callbackString, formattedDuration, stackTraceLines));
			}

			return detailsList;
		}

		@JmxAttribute
		public boolean getStoreStackTrace() {
			return CallbackRegistry.getStoreStackTrace();
		}

		@JmxAttribute
		public void setStoreStackTrace(boolean store) {
			CallbackRegistry.setStoreStackTrace(store);
		}

		@JmxAttribute
		public int getMaxCallbacksToShow() {
			return maxCallbacksToShow;
		}

		@JmxAttribute
		public void setMaxCallbacksToShow(int maxCallbacksToShow) {
			if (maxCallbacksToShow < 0) {
				throw new IllegalArgumentException("argument must be non-negative");
			}
			this.maxCallbacksToShow = maxCallbacksToShow;
		}

		@JmxAttribute
		public int getTotalActiveCallbacks() {
			return CallbackRegistry.REGISTRY.size();
		}

		private static List<Map.Entry<Object, CallbackMetaData>> sortByTimestamp(Map<Object, CallbackMetaData> map) {
			List<Map.Entry<Object, CallbackMetaData>> sortedEntries = new ArrayList<>(map.entrySet());
			Collections.sort(sortedEntries, new Comparator<Map.Entry<Object, CallbackMetaData>>() {
				@Override
				public int compare(Map.Entry<Object, CallbackMetaData> o1, Map.Entry<Object, CallbackMetaData> o2) {
					long ts1 = o1.getValue().getTimestamp();
					long ts2 = o2.getValue().getTimestamp();
					return Long.compare(ts1, ts2);
				}
			});
			return sortedEntries;
		}

		public static final class CallbackDetails {
			private final String className;
			private final String stringRepresentation;
			private final String duration;
			private final List<String> stackTrace;

			public CallbackDetails(String className, String stringRepresentation,
			                       String duration, List<String> stackTrace) {
				this.className = className;
				this.stringRepresentation = stringRepresentation;
				this.duration = duration;
				this.stackTrace = stackTrace;
			}

			@JmxAttribute
			public String getClassName() {
				return className;
			}

			@JmxAttribute
			public String getStringRepresentation() {
				return stringRepresentation;
			}

			@JmxAttribute
			public String getDuration() {
				return duration;
			}

			@JmxAttribute
			public List<String> getStackTrace() {
				return stackTrace;
			}
		}
	}
	// endregion
}
