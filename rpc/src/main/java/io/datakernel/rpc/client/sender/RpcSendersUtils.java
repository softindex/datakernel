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

	static int countPresentSenders(List<RpcRequestSenderHolder> holders) {
		int counter = 0;
		for (RpcRequestSenderHolder holder : holders) {
			if (holder.isSenderPresent()) {
				++counter;
			}
		}
		return counter;
	}

	static List<RpcRequestSender> filterAbsent(List<RpcRequestSenderHolder> holders) {
		List<RpcRequestSender> filtered = new ArrayList<>();
		for (RpcRequestSenderHolder holder : holders) {
			if (holder.isSenderPresent()) {
				filtered.add(holder.getSender());
			}
		}
		return filtered;
	}

	static List<RpcRequestSender> replaceAbsentToNull(List<RpcRequestSenderHolder> holders) {
		List<RpcRequestSender> afterReplacingList = new ArrayList<>();
		for (RpcRequestSenderHolder holder : holders) {
			afterReplacingList.add(holder.getSenderOrNull());
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
