package io.global.globalfs.api;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.global.common.SignedData;

import static io.datakernel.util.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DataFrame {
	@Nullable
	private final ByteBuf buf;

	@Nullable
	private final SignedData<GlobalFsCheckpoint> checkpoint;

	private DataFrame(@Nullable ByteBuf buf, @Nullable SignedData<GlobalFsCheckpoint> checkpoint) {
		assert buf != null ^ checkpoint != null;
		this.buf = buf;
		this.checkpoint = checkpoint;
	}

	public static DataFrame of(ByteBuf buf) {
		return new DataFrame(buf, null);
	}

	public static DataFrame of(SignedData<GlobalFsCheckpoint> checkpoint) {
		return new DataFrame(null, checkpoint);
	}

	public boolean isBuf() {
		return buf != null;
	}

	public boolean isCheckpoint() {
		return checkpoint != null;
	}

	public ByteBuf getBuf() {
		checkState(isBuf());
		assert buf != null;
		return buf;
	}

	public SignedData<GlobalFsCheckpoint> getCheckpoint() {
		checkState(isCheckpoint());
		assert checkpoint != null;
		return checkpoint;
	}

	@Override
	public String toString() {
		assert !isBuf() || buf != null;
		return "DataFrame{" + (isBuf() ? buf.getString(UTF_8).replace("\n", "\\n")/*"buf=(size=" + buf.readRemaining() + ")"*/ : "checkpoint=" + checkpoint) + '}';
	}
}
