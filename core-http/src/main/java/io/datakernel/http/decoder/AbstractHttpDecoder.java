package io.datakernel.http.decoder;

/**
 * Abstract implementation of {@link HttpDecoder} that allows to give its id as
 * a constructor parameter instead of implementing a getter for it.
 */
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
