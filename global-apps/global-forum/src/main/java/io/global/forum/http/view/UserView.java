package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.BanState;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserId;
import io.global.comm.pojo.UserRole;
import io.global.common.CryptoUtils;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class UserView {
	private final String id;
	private final String name;
	private final UserRole role;
	private final String avatarUrl;

	private final String email;
	private final String firstName;
	private final String lastName;
	private final BanState banState;

	public UserView(String id, String name, UserRole role, String avatarUrl, String email, String firstName, String lastName, BanState banState) {
		this.id = id;
		this.name = name;
		this.role = role;
		this.avatarUrl = avatarUrl;
		this.email = email;
		this.firstName = firstName;
		this.lastName = lastName;
		this.banState = banState;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public UserRole getRole() {
		return role;
	}

	public String getAvatarUrl() {
		return avatarUrl;
	}

	public String getEmail() {
		return email;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public BanState getBanState() {
		return banState;
	}

	@Override
	public String toString() {
		return name;
	}

	public static UserView from(UserId userId, @Nullable UserData userData) {
		return userData == null ?
				new UserView(userId.getAuthId(), "ghost", UserRole.GUEST, "https://gravatar.com/avatar?d=mp", null, null, null, null) :
				new UserView(
						userId.getAuthId(),
						userData.getUsername(),
						userData.getRole(),
						avatarUrl(userData),
						userData.getEmail(),
						userData.getFirstName(),
						userData.getLastName(),
						userData.getBanState());
	}

	public static Promise<UserView> from(CommDao commDao, @Nullable UserId userId) {
		return userId == null ?
				Promise.of(null) :
				commDao.getUser(userId)
						.map(userData -> from(userId, userData));
	}

	private static final MessageDigest MD5;

	static {
		try {
			MD5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignored) {
			throw new AssertionError("Apparently, MD5 algorithm does not exist");
		}
	}

	private static String md5(String str) {
		// we are single-threaded so far
//		synchronized (MD5) {
		MD5.update(str.getBytes());
		return CryptoUtils.toHexString(MD5.digest());
//		}
	}

	private static String avatarUrl(UserData data) {
		String email = data.getEmail();
		return email != null ?
				"https://gravatar.com/avatar/" + md5(email) + "?d=identicon" :
				"https://gravatar.com/avatar?d=mp";
	}
}
