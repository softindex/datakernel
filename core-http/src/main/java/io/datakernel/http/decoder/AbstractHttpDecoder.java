package io.datakernel.http.decoder;

public abstract class AbstractHttpDecoder<R> implements HttpDecoder<R> {
	private final String id;

	public AbstractHttpDecoder(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return id;
	}
}
