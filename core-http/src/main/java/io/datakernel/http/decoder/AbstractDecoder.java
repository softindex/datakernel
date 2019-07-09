package io.datakernel.http.decoder;

/**
 * Abstract implementation of {@link Decoder} that allows to give its id as
 * a constructor parameter instead of implementing a getter for it.
 */
public abstract class AbstractDecoder<R> implements Decoder<R> {
	private final String id;

	public AbstractDecoder(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return id;
	}
}
