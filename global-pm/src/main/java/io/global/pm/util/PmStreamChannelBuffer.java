package io.global.pm.util;

import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.queue.ChannelBuffer;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

public final class PmStreamChannelBuffer implements ChannelQueue<SignedData<RawMessage>> {
	public static final int MAX_BUFFER_SIZE = 1024;

	private final ChannelBuffer<SignedData<RawMessage>> buffer;
	private final long timestamp;
	private final Set<Long> messageIds = new HashSet<>();
	private final Set<Long> tombstoneIds = new HashSet<>();

	public PmStreamChannelBuffer(long timestamp) {
		this.buffer = new ChannelBuffer<>(MAX_BUFFER_SIZE);
		this.timestamp = timestamp;
	}

	public void update(SignedData<RawMessage> signedData) {
		long id = signedData.getValue().getId();
		if (signedData.getValue().isMessage()) {
			messageIds.add(id);
		} else {
			messageIds.remove(id);
			tombstoneIds.add(id);
		}
	}

	@Override
	public Promise<Void> put(SignedData<RawMessage> signedData) {
		RawMessage value = signedData.getValue();
		long id = value.getId();
		if (value.getTimestamp() < timestamp || tombstoneIds.contains(id) || messageIds.contains(id) && value.isMessage()) {
			return Promise.complete();
		}
		if (buffer.size() == MAX_BUFFER_SIZE){
			close(new StacklessException(PmStreamChannelBuffer.class, "Buffer overflow"));
		}
		update(signedData);
		return buffer.put(signedData);
	}

	@Override
	public Promise<SignedData<RawMessage>> take() {
		return buffer.take();
	}

	@Override
	public void close(@NotNull Throwable e) {
		buffer.close(e);
	}
}
