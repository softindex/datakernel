package io.global.appstore.pojo;

public final class AppInfo {
	private final int id;
	private final String name;
	private final String description;

	public AppInfo(int id, String name, String description) {
		this.id = id;
		this.name = name;
		this.description = description;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public String toString() {
		return "AppInfo{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
	}
}
