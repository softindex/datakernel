package io.global.blog.util;

import io.datakernel.common.parse.ParseException;
import io.datakernel.http.decoder.Decoder;
import io.datakernel.promise.Promise;
import io.global.blog.http.PublicServlet;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.http.decoder.Decoders.ofGet;

public final class HttpUtil {
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";
	public static Promise<Void> validate(String param, int maxLength, String paramName) {
		return validate(param, maxLength, paramName, false);
	}

	public static Promise<Void> validate(@Nullable String param, int maxLength, String paramName, boolean required) {
		if (param == null && required || (param != null && param.matches(WHITESPACE) && required)) {
			return Promise.ofException(new ParseException(PublicServlet.class, "'" + paramName + "' POST parameter is required"));
		}
		return param != null && param.length() > maxLength ?
				Promise.ofException(new ParseException(PublicServlet.class, paramName + " is too long (" + param.length() + ">" + maxLength + ")")) :
				Promise.complete();
	}

	public static final Decoder<Pagination> PAGINATION_DECODER = Decoder.of(Pagination::new,
			ofGet("page")
					.map(Integer::parseInt)
					.validate(val -> val > 0, "Cannot be negative"),
			ofGet("size")
					.map(Integer::parseInt)
					.validate(val -> val > 0, "Cannot be negative")
	);

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
