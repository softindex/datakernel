package io.datakernel.http;

public abstract class HttpSettings {
	public int maxKeepAliveRequests;
	public int maxHttpMessageSize;

	public static class HttpSettingsClient extends HttpSettings {
	}

	public static class HttpSettingsServer extends HttpSettings {
	}

}
