package io.datakernel.vlog.handler;

import io.datakernel.promise.Promise;

public interface VideoPosterHandler {
	Promise<Void> handle(String videoSourceName);
}
