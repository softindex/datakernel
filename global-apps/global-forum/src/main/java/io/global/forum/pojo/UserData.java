package io.global.forum.pojo;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class UserData {
	private UserRole role;

	@Nullable
	private String email;
	@Nullable
	private String name;
	@Nullable
	private BanState banState = null;

	public UserData(@Nullable String email, @Nullable String name, UserRole role) {
		this.email = email;
		this.name = name;
		this.role = role;
	}

	@Nullable
	public String getEmail() {
		return email;
	}

	public void setEmail(@Nullable String email) {
		this.email = email;
	}

	@Nullable
	public String getName() {
		return name;
	}

	public void setName(@Nullable String name) {
		this.name = name;
	}

	public UserRole getRole() {
		return role;
	}

	public void setRole(UserRole role) {
		this.role = role;
	}

	@Nullable
	public BanState getBanState() {
		return banState;
	}

	public void setBanState(@Nullable BanState banState) {
		this.banState = banState;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		UserData data = (UserData) o;

		if (role != data.role) return false;
		if (!Objects.equals(email, data.email)) return false;
		if (!Objects.equals(name, data.name)) return false;
		return Objects.equals(banState, data.banState);
	}

	@Override
	public int hashCode() {
		int result = role.hashCode();
		result = 31 * result + (email != null ? email.hashCode() : 0);
		result = 31 * result + (name != null ? name.hashCode() : 0);
		result = 31 * result + (banState != null ? banState.hashCode() : 0);
		return result;
	}
}
