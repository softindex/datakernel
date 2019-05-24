package io.datakernel.http.parser;

import io.datakernel.exception.ParseException;
import io.datakernel.util.ParserFunction;

public interface ParserFunctions {
	static ParserFunction<String, Integer> integerParser() {
		return param -> {
			try {
				return Integer.valueOf(param);
			} catch (IllegalArgumentException e) {
				throw new ParseException();
			}
		};
	}

	static ParserFunction<String, Double> doubleParser() {
		return param -> {
			try {
				return Double.valueOf(param);
			} catch (IllegalArgumentException e) {
				throw new ParseException();
			}
		};
	}
}
