package io.global.photos.util;

import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.photos.http.view.AlbumView;
import io.global.photos.http.view.PhotoView;
import io.global.photos.ot.Album;
import io.global.photos.ot.Photo;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Integer.MAX_VALUE;

public final class ViewUtil {
	private static final Function<String, Promise<String>> EMPTY_BASE_64_FUNCTION = $ -> Promise.of(null);
	public static Promise<Map<String, PhotoView>> photoViewMapFrom(Map<String, Photo> photoMap, Function<String, Promise<String>> base64Function) {
		return photoViewFrom(photoMap, 0, MAX_VALUE, base64Function);
	}

	public static Promise<Map<String, PhotoView>> photoViewFrom(Map<String, Photo> photoMap, long offset, Integer size, Function<String, Promise<String>> base64Function) {
		Map<String, PhotoView> photoViewMap = new HashMap<>();
		return Promises.all(photoMap.entrySet()
				.stream()
				.skip(offset)
				.limit(size)
				.map(entry -> {
					Photo photo = entry.getValue();
					return base64Function.apply(entry.getKey())
							.whenResult(base64Image -> photoViewMap.put(entry.getKey(), PhotoView.of(photo, base64Image)));
				}))
				.map($ -> photoViewMap);
	}

	public static Promise<Map<String, AlbumView>> albumViewFrom(Map<String, Album> albums) {
		return albumViewFrom(albums, EMPTY_BASE_64_FUNCTION);
	}

	public static Promise<Map<String, AlbumView>> albumViewFrom(Map<String, Album> albums, Function<String, Promise<String>> base64Function) {
		HashMap<String, AlbumView> map = new HashMap<>();
		return Promises.all(albums.entrySet()
				.stream()
				.map(entry -> {
					String albumId = entry.getKey();
					Album album = entry.getValue();
					return photoViewMapFrom(album.getPhotoMap(), base64Function)
							.whenResult(photoMap -> {
								AlbumView albumView = AlbumView.of(albumId, album.getTitle(), album.getDescription(), photoMap);
								map.put(albumId, albumView);
							});
				}))
				.map($ -> map);
	}

	public static Promise<AlbumView> albumViewFrom(String id, Album album, Function<String, Promise<String>> base64Function) {
		return albumViewFrom(id, album, 0, MAX_VALUE, base64Function);
	}

	/**
	 * @param page should start from 0
	 */
	public static Promise<AlbumView> albumViewFrom(String id, Album album, Integer page, Integer size, Function<String, Promise<String>> base64Function) {
		long res = page.longValue() * size.longValue();
		long offset = ((int) res != res) ? MAX_VALUE : res;
		return photoViewFrom(album.getPhotoMap(), offset, size, base64Function)
				.map(photoViewMap -> AlbumView.of(id, album.getTitle(), album.getDescription(), photoViewMap));
	}
}
