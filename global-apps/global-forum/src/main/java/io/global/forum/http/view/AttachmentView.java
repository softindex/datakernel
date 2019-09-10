package io.global.forum.http.view;

public final class AttachmentView {
	private final String filename;
	private final String globalFsId;

	public AttachmentView(String filename, String globalFsId) {
		this.filename = filename;
		this.globalFsId = globalFsId;
	}

	public String getFilename() {
		return filename;
	}

	public String getGlobalFsId() {
		return globalFsId;
	}
}
