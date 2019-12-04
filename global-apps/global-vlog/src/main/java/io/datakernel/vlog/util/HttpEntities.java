package io.datakernel.vlog.util;

import io.datakernel.http.decoder.Decoder;
import io.datakernel.http.decoder.Validator;
import io.global.comm.pojo.UserData;
import io.global.comm.pojo.UserRole;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static io.datakernel.http.decoder.Decoders.*;

public final class HttpEntities {
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";
	public static final Decoder<AppMetadataParams> APP_METADATA_DECODER = Decoder.of(AppMetadataParams::new,
			ofPost("title")
					.validate(value -> value == null || value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(32)),
			ofPost("description")
					.validate(value -> value == null || value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(1024))
	);
	public static final Decoder<ThreadMetadata> THREAD_DECODER = Decoder.of(ThreadMetadata::new,
			ofPost("title")
					.validate(value -> value == null || !value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(32)),
			ofPost("content")
					.validate(value -> value == null || !value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(1024))
	);
	public static final Decoder<ProfileMetadata> PROFILE_DECODER = Decoder.of(ProfileMetadata::new,
			ofPost("email")
					.validate(Objects::nonNull, "Is required")
					.validate(value -> !value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(64)),
			ofPost("username")
					.validate(Objects::nonNull, "Is required")
					.validate(value -> !value.matches(WHITESPACE), "Cannot be empty")
					.validate(lenghtValidator(64)),
			ofPost("firstName")
					.validate(lenghtValidator(64)),
			ofPost("lastName")
					.validate(lenghtValidator(64))
	);

	public static final Decoder<Pagination> PAGINATION_DECODER = Decoder.of(Pagination::new,
			ofGet("page")
					.map(Integer::parseInt)
					.validate(val -> val > 0, "Cannot be negative"),
			ofGet("size")
					.map(Integer::parseInt)
					.validate(val -> val > 0, "Cannot be negative")
	);

	public static final Decoder<Comment> COMMENT_DECODER = Decoder.of(Comment::new,
			ofPath("commentID"),
			ofPost("content")
					.validate(Objects::nonNull, "Is required")
					.validate(lenghtValidator(1024))
	);

	private static Validator<String> lenghtValidator(int limit) {
		return Validator.of(value -> value == null || value.length() <= limit, "Is too large > " + limit);
	}

	public static class AppMetadataParams {
		private final String title;
		private final String description;

		public AppMetadataParams(@Nullable String title, @Nullable String description) {
			this.title = title;
			this.description = description;
		}

		@Nullable
		public String getTitle() {
			return title;
		}

		@Nullable
		public String getDescription() {
			return description;
		}
	}

	public static class ThreadMetadata {
		private final String title;
		private final String description;

		public ThreadMetadata(String title, String description) {
			this.title = title;
			this.description = description;
		}

		public String getTitle() {
			return title;
		}

		public String getDescription() {
			return description;
		}
	}

	public static class ProfileMetadata {
		private final String email;
		private final String username;
		private final String firstName;
		private final String lastName;

		public ProfileMetadata(String email, String username, String firstName, String lastName) {
			this.email = email;
			this.username = username;
			this.firstName = firstName;
			this.lastName = lastName;
		}

		public String getEmail() {
			return email;
		}

		public String getUsername() {
			return username;
		}

		public String getFirstName() {
			return firstName;
		}

		public String getLastName() {
			return lastName;
		}

		public UserData toUserData(UserRole role) {
			return new UserData(role, email, username, firstName, lastName);
		}
	}

	public static class Comment {
		private final String parentId;
		private final String content;

		public Comment(String parentId, String content) {
			this.parentId = parentId;
			this.content = content;
		}

		public String getParentId() {
			return parentId;
		}

		public String getContent() {
			return content;
		}
	}

	public static class Pagination {
		private final int page;
		private final int size;

		public Pagination(int page, int size) {
			this.page = page;
			this.size = size;
		}

		public int getPage() {
			return page;
		}

		public int getSize() {
			return size;
		}
	}
}
