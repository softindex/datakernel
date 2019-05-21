package io.datakernel.di;

public final class LocationInfo {
	private final StackTraceElement position;

	private LocationInfo(StackTraceElement position) {
		this.position = position;
	}

	public static LocationInfo here(int level) {
		return new LocationInfo(Thread.currentThread().getStackTrace()[level + 2]);
	}

	public static LocationInfo here() {
		return here(1); // level is 1 because we are by one `here()` method call deeper, huh
	}

	public StackTraceElement getPosition() {
		return position;
	}
}
