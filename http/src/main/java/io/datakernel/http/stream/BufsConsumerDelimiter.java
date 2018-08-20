package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

public final class BufsConsumerDelimiter implements BufsConsumer {
	private final BufsConsumer next;

	private final ByteBufQueue outputBufs = new ByteBufQueue();
	private int remaining;

	public BufsConsumerDelimiter(BufsConsumer next, int remaining) {
		this.next = next;
		this.remaining = remaining;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		remaining -= inputBufs.drainTo(outputBufs, remaining);
		if (endOfStream && remaining != 0) {
			return Stage.ofException(new ParseException("Unexpected end-of-stream"));
		}
		return next.push(outputBufs, remaining == 0);
	}

	@Override
	public void closeWithError(Throwable e) {
		next.closeWithError(e);
	}

}
