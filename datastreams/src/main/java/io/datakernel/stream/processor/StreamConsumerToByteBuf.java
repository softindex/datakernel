package io.datakernel.stream.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;

public class StreamConsumerToByteBuf extends AbstractStreamConsumer<ByteBuf> implements StreamConsumerWithResult<ByteBuf, ByteBuf> {

	@Nullable
	private ByteBuf state = null;
	private SettableStage<ByteBuf> result = new SettableStage<>();

	private StreamConsumerToByteBuf() {
	}

	public static StreamConsumerToByteBuf create() {
		return new StreamConsumerToByteBuf();
	}

	@Override
	protected void onStarted() {
		getProducer().produce(buf -> {
			ByteBuf state = this.state;
			if (state == null) {
				state = ByteBufPool.allocate(buf.readRemaining());
			}
			state = ByteBufPool.ensureWriteRemaining(state, buf.readRemaining());
			state.put(buf);
			buf.recycle();
			this.state = state;
		});
	}

	@Override
	protected void onEndOfStream() {
		result.set(state != null ? state : ByteBuf.empty());
	}

	@Override
	protected void onError(Throwable t) {
	}

	@Override
	public MaterializedStage<ByteBuf> getResult() {
		return result;
	}
}
