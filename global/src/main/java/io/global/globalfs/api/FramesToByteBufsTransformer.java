package io.global.globalfs.api;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.globalfs.api.GlobalFsCheckpoint.CheckpointVerificationResult;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;

import static io.datakernel.util.Preconditions.checkState;

public abstract class FramesToByteBufsTransformer
		extends AbstractAsyncProcess
		implements WithSerialToSerial<FramesToByteBufsTransformer, DataFrame, ByteBuf> {
	private final PubKey pubKey;

	protected SerialSupplier<DataFrame> input;
	protected SerialConsumer<ByteBuf> output;
	protected long position = 0;

	private boolean first = true;
	private SHA256Digest digest;

	public FramesToByteBufsTransformer(PubKey pubKey) {
		this.pubKey = pubKey;
	}

	@Override
	public void setInput(SerialSupplier<DataFrame> input) {
		checkState(this.input == null, "Input is already set");
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		checkState(this.output == null, "Output is already set");
		this.output = output;
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(frame -> {
					if (frame == null) {
						output.accept(null);
						return;
					}
					handleFrame(frame)
							.whenComplete(($, e) -> {
								if (e != null) {
									input.closeWithError(e);
									return;
								}
								doProcess();
							});
				});
	}

	protected Stage<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
		return Stage.of(null);
	}

	protected Stage<Void> receiveByteBuffer(ByteBuf byteBuf) {
		return output.accept(byteBuf);
	}

	private Stage<Void> handleFrame(DataFrame frame) {
		if (first) {
			first = false;
			if (!frame.isCheckpoint()) {
				return Stage.ofException(new IOException("First dataframe is not a checkpoint!"));
			}
			GlobalFsCheckpoint data = frame.getCheckpoint().getData();
			position = data.getPosition();
			digest = new SHA256Digest(data.getDigest());
		}
		if (frame.isCheckpoint()) {
			SignedData<GlobalFsCheckpoint> checkpoint = frame.getCheckpoint();
			CheckpointVerificationResult result = GlobalFsCheckpoint.verify(checkpoint, pubKey, position, digest);
			if (result != CheckpointVerificationResult.SUCCESS) {
				return Stage.ofException(new IOException("Checkpoint verification failed: " + result.message));
			}
//			return output.post(ByteBuf.wrapForReading(new byte[]{124}));
			return receiveCheckpoint(checkpoint);
		}
		ByteBuf buf = frame.getBuf();
		int size = buf.readRemaining();
		position += size;
		digest.update(buf.array(), buf.readPosition(), size);
		return receiveByteBuffer(buf);
	}

	@Override
	protected final void doCloseWithError(Throwable e) {
		if (input != null) {
			input.closeWithError(e);
		}
		if (output != null) {
			output.closeWithError(e);
		}
	}

}
