package io.datakernel.http;

public final class QueryParameter {
	private final String key;
	private final String value;

	QueryParameter(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		QueryParameter queryParameter = (QueryParameter) o;

		if (key != null ? !key.equals(queryParameter.key) : queryParameter.key != null) return false;
		return value != null ? value.equals(queryParameter.value) : queryParameter.value == null;
	}

	@Override
	public int hashCode() {
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + (value != null ? value.hashCode() : 0);
		return result;
	}
}
