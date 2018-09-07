package io.global.globalfs.api;

import io.datakernel.remotefs.FileMetadata;

import java.util.List;

public final class Revision {
	private final long revisionId;
	private final List<FileMetadata> list;

	public Revision(long revisionId, List<FileMetadata> list) {
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
