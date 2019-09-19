package io.global.appstore.pojo;

import io.global.common.PubKey;

public final class Profile {
	private final PubKey pubKey;
	private final User user;
	private final String email;

	public Profile(PubKey pubKey, User user, String email) {
		this.pubKey = pubKey;
		this.user = user;
		this.email = email;
	}

	public Profile(PubKey pubKey, String username, String firstName, String lastName, String email) {
		this(pubKey, new User(username, firstName, lastName), email);
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getUsername() {
		return user.getUsername();
	}

	public String getFirstName() {
		return user.getFirstName();
	}

	public String getLastName() {
		return user.getLastName();
	}

	public User getUser() {
		return user;
	}

	public String getEmail() {
		return email;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Profile profile = (Profile) o;

		if (!pubKey.equals(profile.pubKey)) return false;
		if (!user.equals(profile.user)) return false;
		if (!email.equals(profile.email)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = pubKey.hashCode();
		result = 31 * result + user.hashCode();
		result = 31 * result + email.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Profile{" +
				"pubKey=" + pubKey +
				", user=" + user +
				", email='" + email + '\'' +
				'}';
	}
}
