package io.datakernel.examples.decoder;

final class Role {
	private final String title;

	public Role(String title) {
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	@Override
	public String toString() {
		return "Role{" +
				"title='" + title + '\'' +
				'}';
	}
}
