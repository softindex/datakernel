package io.global.appstore.pojo;

public final class AppInfo {
	private final int id;
	private final String name;
	private final String description;
	private final String logoUrl;

	public AppInfo(int id, String name, String description, String logoUrl) {
		this.id = id;
		this.name = name;
		this.description = description;
		this.logoUrl = logoUrl;
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

	public String getLogoUrl() {
		return logoUrl;
	}

	@Override
	public String toString() {
		return "AppInfo{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
	}
}
