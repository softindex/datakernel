package io.datakernel.common;

@SuppressWarnings("unchecked")
public interface Initializable<T extends Initializable<T>> {

	default T initialize(Initializer<? super T> initializer) {
		initializer.accept((T) this);
		return (T) this;
	}

}
