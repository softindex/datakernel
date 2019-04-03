package io.datakernel.remotefs;

import java.nio.file.Path;

public interface FileNamingScheme {

	String encode(String name, long revision, boolean tombstone);

	FilenameInfo decode(Path path, String name);

	class FilenameInfo {
		private final Path filePath;
		private final String name;
		private final long revision;
		private final boolean tombstone;

		public FilenameInfo(Path filePath, String name, long revision, boolean tombstone) {
			this.filePath = filePath;
			this.name = name;
			this.revision = revision;
			this.tombstone = tombstone;
		}

		public Path getFilePath() {
			return filePath;
		}

		public String getName() {
			return name;
		}

		public long getRevision() {
			return revision;
		}

		public boolean isTombstone() {
			return tombstone;
		}
	}
}
