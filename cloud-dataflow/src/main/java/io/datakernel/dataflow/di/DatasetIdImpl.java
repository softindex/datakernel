package io.datakernel.dataflow.di;

import io.datakernel.di.Key;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class DatasetIdImpl implements DatasetId {
	@NotNull
	private final String value;

	private DatasetIdImpl(@NotNull String value) {
		this.value = value;
	}

	public static Key<Object> datasetId(String id) {
		return Key.of(Object.class, new DatasetIdImpl(id));
	}

	@NotNull
	@Override
	public String value() {
		return value;
	}

	@Override
	public int hashCode() {
		// This is specified in java.lang.Annotation.
		return (127 * "value".hashCode()) ^ value.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof DatasetId)) return false;

		DatasetId other = (DatasetId) o;
		return value.equals(other.value());
	}

	@NotNull
	@Override
	public String toString() {
		return "@" + DatasetId.class.getName() + "(" + value + ")";
	}

	@NotNull
	@Override
	public Class<? extends Annotation> annotationType() {
		return DatasetId.class;
	}
}
