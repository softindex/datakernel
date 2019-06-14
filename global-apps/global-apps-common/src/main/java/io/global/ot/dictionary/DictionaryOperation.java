package io.global.ot.dictionary;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;

public final class DictionaryOperation {
	private final Map<String, SetOperation> operations;

	private DictionaryOperation(Map<String, SetOperation> operations) {
		this.operations = operations;
	}

	public static DictionaryOperation of(Map<String, SetOperation> operations) {
		return new DictionaryOperation(operations);
	}

	public static DictionaryOperation forKey(String key, SetOperation operation) {
		return new DictionaryOperation(singletonMap(key, operation));
	}

	public Map<String, SetOperation> getOperations() {
		return Collections.unmodifiableMap(operations);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DictionaryOperation that = (DictionaryOperation) o;

		if (!operations.equals(that.operations)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return operations.hashCode();
	}

}
