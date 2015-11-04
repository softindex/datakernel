package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

final class Utils {

	private Utils() {}

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
}
