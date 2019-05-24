package io.datakernel.http.parser;

import java.util.Collections;
import java.util.Set;

abstract class HttpParamParserImpl<T> implements HttpParamParser<T> {
	@Override
	public Set<String> getIds() {
		return Collections.emptySet();
	}
}
