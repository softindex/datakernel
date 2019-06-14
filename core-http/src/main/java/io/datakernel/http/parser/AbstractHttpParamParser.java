package io.datakernel.http.parser;

public abstract class AbstractHttpParamParser<R> implements HttpParamParser<R> {
	private final String id;

	public AbstractHttpParamParser(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return id;
	}
}
