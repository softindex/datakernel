package io.datakernel.vlog.handler;

import io.datakernel.promise.Promise;

public interface VideoHandler {
	Promise<Void> handle(String filename);

	String getName();
}
