package io.global.ot.dictionary;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTState;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DictionaryOTState implements OTState<DictionaryOperation> {
	private final Map<@NotNull String, @NotNull String> dictionary = new HashMap<>();

	@Override
	public Promise<Void> init() {
		dictionary.clear();
		return Promise.complete();
	}

	@Override
	public Promise<Void> apply(DictionaryOperation dictionaryOperation) {
		dictionaryOperation.getOperations()
				.forEach((key, op) -> {
					String next = op.getNext();
					if (next == null) {
						dictionary.remove(key);
					} else {
						dictionary.put(key, next);
					}
				});
		return Promise.complete();
	}

	public Map<String, String> getDictionary() {
		return Collections.unmodifiableMap(dictionary);
	}
}
