package io.global.forum.http.view;

import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserRole;
import io.global.common.CryptoUtils;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public final class UserView {
	private final String id;
	private final String name;
	private final UserRole role;
	private final String avatarUrl;

	private final String email;
	private final String firstName;
	private final String lastName;

	private BanView ban;

	public UserView(String id, String name, UserRole role, String avatarUrl, String email, String firstName, String lastName) {
		this.id = id;
		this.name = name;
		this.role = role;
		this.avatarUrl = avatarUrl;
		this.email = email;
		this.firstName = firstName;
		this.lastName = lastName;
	}

	public String getUserId() {
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

	public BanView getBan() {
		return ban;
	}

	public void setBan(BanView ban) {
		this.ban = ban;
	}

	@Override
	public String toString() {
		return name;
	}

	static Promise<UserView> from(CommDao commDao, UserId userId, @Nullable UserData userData, Map<UserId, UserView> cache) {
		UserView cached = cache.get(userId);
		if (cached != null) {
			return Promise.of(cached);
		}
		if (userData == null) {
			return Promise.of(new UserView(userId.getAuthId(), "ghost", UserRole.GUEST, "https://gravatar.com/avatar?d=mp", null, null, null));
		}
		UserView view = new UserView(
				userId.getAuthId(),
				userData.getUsername(),
				userData.getRole(),
				avatarUrl(userData),
				userData.getEmail(),
				userData.getFirstName(),
				userData.getLastName());
		cache.put(userId, view);
		return BanView.from(commDao, userData.getBanState(), cache)
				.map(ban -> {
					view.setBan(ban);
					return view;
				});
	}

	public static Promise<UserView> from(CommDao commDao, UserId userId, @Nullable UserData userData) {
		return from(commDao, userId, userData, new HashMap<>());
	}

	public static Promise<List<UserView>> from(CommDao commDao, int page, int limit) {
		return commDao.getUsers().slice(page * limit, limit)
				.then(users -> Promises.toList(users.stream().map(e -> from(commDao, e.getKey(), e.getValue()))))
				.map(list -> list.stream().filter(Objects::nonNull).collect(toList()));
	}

	public static Promise<UserView> from(CommDao commDao, @Nullable UserId userId) {
		return userId != null ?
				commDao.getUsers().get(userId).then(userData -> from(commDao, userId, userData)) :
				Promise.of(null);
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
