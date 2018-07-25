package io.global.globalfs.api;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.global.common.PubKey;
import io.global.common.SignedData;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkState;

public interface GlobalFsServer {
	Stage<Set<String>> getFsNames(PubKey pubKey);

	final class DataFrame {
		@Nullable
		private final ByteBuf buf;

		@Nullable
		private final SignedData<GlobalFsCheckpoint> checkpoint;

		private DataFrame(ByteBuf buf, SignedData<GlobalFsCheckpoint> checkpoint) {
			this.buf = buf;
			this.checkpoint = checkpoint;
		}

		public static DataFrame ofByteBuf(ByteBuf buf) {
			return new DataFrame(buf, null);
		}

		public static DataFrame ofCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
			return new DataFrame(null, checkpoint);
		}

		public boolean isBuf() {
			return !isCheckpoint();
		}

		public boolean isCheckpoint() {
			return checkpoint != null;
		}

		public ByteBuf getBuf() {
			checkState(isBuf());
			return buf;
		}

		public SignedData<GlobalFsCheckpoint> getCheckpoint() {
			checkState(isCheckpoint());
			return checkpoint;
		}
	}

	final class FilesWithSize {
		private final long revisionId;
		private final List<FileMetadata> list;

		public FilesWithSize(long revisionId, List<FileMetadata> list) {
			this.revisionId = revisionId;
			this.list = list;
		}

		public long getRevisionId() {
			return revisionId;
		}

		public List<FileMetadata> getList() {
			return list;
		}
	}

	Stage<StreamProducerWithResult<DataFrame, Void>> download(GlobalFsName id, String filename, long offset, long limit);

	default StreamProducerWithResult<DataFrame, Void> downloadStream(GlobalFsName id, String filename, long offset, long limit) {
		return StreamProducerWithResult.ofStage(download(id, filename, offset, limit));
	}

	Stage<StreamConsumerWithResult<DataFrame, Void>> upload(GlobalFsName id, String filename, long offset);

	default StreamConsumerWithResult<DataFrame, Void> uploadStream(GlobalFsName id, String filename, long offset) {
		return StreamConsumerWithResult.ofStage(upload(id, filename, offset));
	}

	Stage<FilesWithSize> list(GlobalFsName id, long revisionId);

	Stage<List<FileMetadata>> list(GlobalFsName id, String glob);

	Stage<Void> delete(GlobalFsName name, String glob);

	Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes);

	Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes);

}
