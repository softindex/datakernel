package io.global.editor.document.name;


public final class ChangeDocumentName {
	private final String prev;
	private final String next;
	private final long timestamp;

	public ChangeDocumentName(String prev, String next, long timestamp) {
		this.prev = prev;
		this.next = next;
		this.timestamp = timestamp;
	}

	public static ChangeDocumentName changeName(String prev, String next, long timestamp) {
		return new ChangeDocumentName(prev, next, timestamp);
	}

	public String getPrev() {
		return prev;
	}

	public String getNext() {
		return next;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isEmpty() {
		return next.equals(prev);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChangeDocumentName that = (ChangeDocumentName) o;

		if (timestamp != that.timestamp) return false;
		if (!prev.equals(that.prev)) return false;
		if (!next.equals(that.next)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = prev.hashCode();
		result = 31 * result + next.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "ChangeDocumentName{" +
				"prev='" + prev + '\'' +
				", next='" + next + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
