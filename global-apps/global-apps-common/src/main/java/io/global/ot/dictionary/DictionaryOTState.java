package io.global.ot.dictionary;

import io.datakernel.ot.OTState;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DictionaryOTState implements OTState<DictionaryOperation> {
	private final Map<@NotNull String, @NotNull String> dictionary = new HashMap<>();

	@Override
	public void init() {
		dictionary.clear();
	}

	@Override
	public void apply(DictionaryOperation dictionaryOperation) {
		Map<String, SetOperation> operations = dictionaryOperation.getOperations();
		operations.forEach((key, op) -> {
			String next = op.getNext();
			if (next == null) {
				dictionary.remove(key);
			} else {
				dictionary.put(key, next);
			}
		});
	}

	public Map<String, String> getDictionary() {
		return Collections.unmodifiableMap(dictionary);
	}
}
