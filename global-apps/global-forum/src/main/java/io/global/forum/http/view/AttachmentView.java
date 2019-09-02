package io.global.forum.http.view;

import io.global.forum.pojo.Attachment;

import java.util.Map;

public final class AttachmentView {
	private final String attachmentType;
	private final String filename;
	private final String globalFsId;

	private AttachmentView(String attachmentType, String filename, String globalFsId) {
		this.attachmentType = attachmentType;
		this.filename = filename;
		this.globalFsId = globalFsId;
	}

	public String getAttachmentType() {
		return attachmentType;
	}

	public String getFilename() {
		return filename;
	}

	public String getGlobalFsId() {
		return globalFsId;
	}

	public static AttachmentView from(Map.Entry<String, Attachment> attachmentEntry) {
		Attachment value = attachmentEntry.getValue();
		return new AttachmentView(value.getAttachmentType().name(), value.getFilename(), attachmentEntry.getKey());
	}
}
