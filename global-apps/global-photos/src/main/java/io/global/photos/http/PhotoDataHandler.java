package io.global.photos.http;

import io.datakernel.csp.ChannelConsumers;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.MultipartParser;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.promise.Promise;
import io.global.photos.dao.AlbumDao;

import java.util.Map;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;

public final class PhotoDataHandler {
	private static final HttpException INAPPROPRIATE_CONTENT_TYPE = HttpException.ofCode(400, "Content type is not multipart/form-data");
	private static final String EMPTY = "";

	public static Promise<Void> handle(HttpRequest request, MultipartDataHandler multipartDataHandler) {
		String contentType = request.getHeader(CONTENT_TYPE);
		return (contentType == null || !contentType.startsWith("multipart/form-data; boundary=")) ?
				Promise.ofException(INAPPROPRIATE_CONTENT_TYPE) :
				MultipartParser.create(getBoundary(contentType))
						.split(request.getBodyStream(), multipartDataHandler);
	}

	public static MultipartDataHandler createMultipartHandler(AlbumDao albumDao, Map<String, String> paramsMap, Map<String, String> photoMap) {
		return MultipartDataHandler.fieldsToMap(paramsMap, (fieldName, fileName) -> fileName.isEmpty() ?
				Promise.of(ChannelConsumers.recycling()) :
				albumDao.generatePhotoId()
						.whenResult(id -> photoMap.put(id, fileName))
						.then(id -> albumDao.addPhoto(id, fileName, EMPTY)));
	}

	private static String getBoundary(String contentType) {
		String boundary = contentType.substring(30);
		if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
			boundary = boundary.substring(1, boundary.length() - 1);
		}
		return boundary;
	}
}
