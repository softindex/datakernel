package io.global.globalfs.server;

import io.datakernel.http.AsyncServlet;
import io.datakernel.http.MiddlewareServlet;

public class GlobalFsNodeServlet {

	public AsyncServlet create() {
		return MiddlewareServlet.create();
	}
}
