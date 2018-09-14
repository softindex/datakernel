package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.global.common.PrivKey;
import io.global.common.SignedData;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.Objects;

public class SignerTransformer extends ByteBufsToFramesTransformer {
	private final SHA256Digest digest = new SHA256Digest();
	private final CheckpointPositionStrategy checkpointPositionStrategy;
	private final PrivKey privateKey;

	private boolean lastPostedCheckpoint = false;

	private SignerTransformer(long offset, CheckpointPositionStrategy checkpointPositionStrategy, PrivKey privateKey) {
		super(offset);
		this.checkpointPositionStrategy = checkpointPositionStrategy;
		this.privateKey = privateKey;
	}

	public static SignerTransformer create(long offset, CheckpointPositionStrategy checkpointPositionStrategy, PrivKey privateKey) {
		return new SignerTransformer(offset, checkpointPositionStrategy, privateKey);
	}

	@Override
	protected Stage<Void> postByteBuf(ByteBuf buf) {
		digest.update(buf.array(), buf.readPosition(), buf.readRemaining());
		lastPostedCheckpoint = false;
//		System.out.println("posting " + buf.asString(UTF_8));
		return super.postByteBuf(buf);
	}

	@Override
	protected Stage<Void> postNextCheckpoint() {
		nextCheckpoint = Objects.requireNonNull(checkpointPositionStrategy, "wtf").nextPosition(nextCheckpoint);
		GlobalFsCheckpoint checkpoint = GlobalFsCheckpoint.of(position, new SHA256Digest(digest));
		lastPostedCheckpoint = true;
//		System.out.println("posting checkpoint at " + position);
		return output.accept(DataFrame.of(SignedData.sign(checkpoint, privateKey)));
	}

	@Override
	protected void iteration() {
		input.get()
				.whenResult(buf -> {
					if (buf == null) {
						if (lastPostedCheckpoint) {
							output.accept(null)
									.thenRun(this::completeProcess);
						} else {
							nextCheckpoint = position;
							postNextCheckpoint()
									.thenRun(() -> output.accept(null))
									.thenRun(this::completeProcess);
						}
						return;
					}
					handleBuffer(buf)
							.whenComplete(($, e) -> {
								if (e != null) {
									closeWithError(e);
								}
								iteration();
							});
				});
	}
}
