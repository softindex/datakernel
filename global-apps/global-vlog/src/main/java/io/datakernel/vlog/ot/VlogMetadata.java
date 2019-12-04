package io.datakernel.vlog.ot;

public final class VlogMetadata {
	public static final VlogMetadata EMPTY = new VlogMetadata("", "");
	private String title;
	private String description;

	public VlogMetadata(String title, String description) {
		this.title = title;
		this.description = description;
	}

	public String getTitle() {
		return title;
	}

	public VlogMetadata setTitle(String title) {
		this.title = title;
		return this;
	}

	public String getDescription() {
		return description;
	}

	public VlogMetadata setDescription(String description) {
		this.description = description;
		return this;
	}
}
