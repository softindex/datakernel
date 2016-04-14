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

package io.datakernel.aggregation_db.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.datakernel.async.ResultCallback;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

public abstract class AsyncResultsTracker<A> {
	protected final ResultCallback<A> callback;

	protected A results;
	protected Exception exception;

	protected int operations;
	protected boolean shutDown;

	private AsyncResultsTracker(ResultCallback<A> callback) {
		this.callback = callback;
	}

	public static class AsyncResultsTrackerList<T> extends AsyncResultsTracker<List<T>> {
		private AsyncResultsTrackerList(ResultCallback<List<T>> callback) {
			super(callback);
			this.results = new ArrayList<>();
		}

		public void completeWithResults(List<T> resultList) {
			--operations;
			results.addAll(resultList);
			returnResultsIfNeeded();
		}

		public void completeWithResult(T result) {
			--operations;
			results.add(result);
			returnResultsIfNeeded();
		}
	}

	public static class AsyncResultsTrackerMultimap<K, V> extends AsyncResultsTracker<Multimap<K, V>> {
		private AsyncResultsTrackerMultimap(ResultCallback<Multimap<K, V>> callback) {
			super(callback);
			this.results = HashMultimap.create();
		}

		public void completeWithResults(K key, List<V> values) {
			--operations;
			results.putAll(key, values);
			returnResultsIfNeeded();
		}

		public void completeWithResult(K key, V value) {
			--operations;
			results.put(key, value);
			returnResultsIfNeeded();
		}
	}

	public static <T> AsyncResultsTrackerList<T> ofList(ResultCallback<List<T>> callback) {
		return new AsyncResultsTrackerList<>(callback);
	}

	public static <K, V> AsyncResultsTrackerMultimap<K, V> ofMultimap(ResultCallback<Multimap<K, V>> callback) {
		return new AsyncResultsTrackerMultimap<>(callback);
	}

	public void startOperation() {
		checkArgument(!shutDown);
		++operations;
	}

	public void completeWithException(Exception e) {
		--operations;
		exception = e;
		returnResultsIfNeeded();
	}

	public void shutDown() {
		shutDown = true;
		returnResultsIfNeeded();
	}

	public void shutDownWithException(Exception e) {
		exception = e;
		shutDown = true;
		returnResultsIfNeeded();
	}

	public int getOperationsCount() {
		return operations;
	}

	protected void returnResultsIfNeeded() {
		if (!shutDown || operations > 0)
			return;

		if (exception != null) {
			callback.fireException(exception);
			return;
		}

		callback.sendResult(results);
	}
}
