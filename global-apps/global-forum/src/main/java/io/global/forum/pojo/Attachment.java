package io.global.forum.pojo;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofEnum;

public final class Attachment {
	public static final StructuredCodec<Attachment> CODEC = StructuredCodecs.tuple(Attachment::new,
			Attachment::getAttachmentType, ofEnum(AttachmentType.class),
			Attachment::getName, STRING_CODEC);

	private final AttachmentType attachmentType;
	private final String fileName;

	public Attachment(AttachmentType attachmentType, String fileName) {
		this.attachmentType = attachmentType;
		this.fileName = fileName;
	}

	public AttachmentType getAttachmentType() {
		return attachmentType;
	}

	public String getName() {
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
