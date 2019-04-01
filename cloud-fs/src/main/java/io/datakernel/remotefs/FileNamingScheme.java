package io.datakernel.remotefs;

import org.jetbrains.annotations.Nullable;

public interface FileNamingScheme {

	String encode(String name, long revision, boolean tombstone);

	@Nullable
	FileMetadata decode(String filename, long size, long timestamp);
}
