package io.global.appstore.pojo;

import org.jetbrains.annotations.Nullable;

public final class HostingInfo {
	private final int id;
	private final String name;
	@Nullable
	private final String logoUrl;
	@Nullable
	private final String description;
	@Nullable
	private final String terms;

	public HostingInfo(int id, String name, @Nullable String description, @Nullable String logoUrl, @Nullable String terms) {
		this.id = id;
		this.name = name;
		this.description = description;
		this.logoUrl = logoUrl;
		this.terms = terms;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Nullable
	public String getDescription() {
		return description;
	}

	@Nullable
	public String getTerms() {
		return terms;
	}

	@Nullable
	public String getLogoUrl() {
		return logoUrl;
	}

	@Override
	public String toString() {
		return "HostingInfo{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
	}
}
