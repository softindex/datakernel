package io.datakernel.http.stream;

import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufQueue;

public interface BufsConsumer extends Cancellable {
	Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream);
}
