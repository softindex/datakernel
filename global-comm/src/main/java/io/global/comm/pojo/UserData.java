package io.global.comm.pojo;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class UserData {
	private UserRole role;

	@Nullable
	private String email;
	@Nullable
	private String username;
	@Nullable
	private String firstName;
	@Nullable
	private String lastName;
	@Nullable
	private BanState banState;

	public UserData(UserRole role, @Nullable String email, @Nullable String username, @Nullable String firstName,
			@Nullable String lastName, @Nullable BanState banState) {
		this.role = role;
		this.email = email;
		this.username = username;
		this.firstName = firstName;
		this.lastName = lastName;
		this.banState = banState;
	}

	public UserData(UserRole role, @Nullable String email, @Nullable String username, @Nullable String firstName,
			@Nullable String lastName) {
		this(role, email, username, firstName, lastName, null);
	}

	public UserRole getRole() {
		return role;
	}

	public void setRole(UserRole role) {
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
	public String getUsername() {
		return username;
	}

	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	@Nullable
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(@Nullable String firstName) {
		this.firstName = firstName;
	}

	@Nullable
	public String getLastName() {
		return lastName;
	}

	public void setLastName(@Nullable String lastName) {
		this.lastName = lastName;
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
		UserData userData = (UserData) o;
		return role == userData.role &&
				Objects.equals(email, userData.email) &&
				Objects.equals(username, userData.username) &&
				Objects.equals(firstName, userData.firstName) &&
				Objects.equals(lastName, userData.lastName) &&
				Objects.equals(banState, userData.banState);
	}

	@Override
	public int hashCode() {
		int result = role.hashCode();
		result = 31 * result + (email != null ? email.hashCode() : 0);
		result = 31 * result + (username != null ? username.hashCode() : 0);
		result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
		result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
		result = 31 * result + (banState != null ? banState.hashCode() : 0);
		return result;
	}
}
