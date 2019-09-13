package io.global.comm.pojo;

public final class Attachment {
	private final AttachmentType attachmentType;
	private final String fileName;

	public Attachment(AttachmentType attachmentType, String fileName) {
		this.attachmentType = attachmentType;
		this.fileName = fileName;
	}

	public AttachmentType getAttachmentType() {
		return attachmentType;
	}

	public String getFilename() {
		return fileName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Attachment that = (Attachment) o;

		if (attachmentType != that.attachmentType) return false;
		if (!fileName.equals(that.fileName)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = attachmentType.hashCode();
		result = 31 * result + fileName.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Attachment{" +
				"[" + attachmentType + "]" +
				'\'' + fileName + "'}";
	}
}
