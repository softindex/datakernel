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

package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

final class RpcSendersUtils {

	private RpcSendersUtils() {}

	static <T> List<T> flatten(List<List<T>> listOfList) {
		List<T> flatList = new ArrayList<>();
		for (List<T> list : listOfList) {
			for (T element : list) {
				flatList.add(element);
			}
		}
		return flatList;
	}

	static <T> int countPresentValues(List<Optional<T>> values) {
		int counter = 0;
		for (Optional<T> value : values) {
			if (value.isPresent()) {
				++counter;
			}
		}
		return counter;
	}

	static <T> List<T> filterAbsent(List<Optional<T>> list) {
		List<T> filtered = new ArrayList<>();
		for (Optional<T> value : list) {
			if (value.isPresent()) {
				filtered.add(value.get());
			}
		}
		return filtered;
	}

	static <T> List<T> replaceAbsentToNull(List<Optional<T>> list) {
		List<T> afterReplacingList = new ArrayList<>();
		for (Optional<T> value : list) {
			afterReplacingList.add(value.orNull());
		}
		return afterReplacingList;
	}

	static <T> boolean containsNullValues(List<T> list) {
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) == null) {
				return true;
			}
		}
		return false;
	}
}
