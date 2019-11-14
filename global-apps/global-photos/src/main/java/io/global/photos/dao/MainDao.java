package io.global.photos.dao;

import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.ot.session.UserId;
import io.global.photos.ot.Album;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface MainDao {
	AlbumDao getAlbumDao(String albumId);

	Promise<String> generateAlbumId();

	Promise<Map<String, Album>> getAlbums(Integer page, Integer size);

	Promise<Map<String, Album>> getAlbums();

	Promise<Album> getAlbum(String id);

	Promise<Void> crateAlbum(String aid, String title, String description);

	Promise<Void> updateAlbum(String aid, @Nullable String title, @Nullable String description);

	Promise<Void> removeAlbum(String aid);

	Promise<Void> movePhotos(String from, String to, Set<String> photoIds);

	Promise<Boolean> albumExist(String aid);

	SessionStore<UserId> getSessionStore();

	KeyPair getKeys();

	Promise<Integer> getAmountPhotos(String albumId);

	Promise<Integer> getAlbumsAmount();

	Promise<String> getBase64(String photoId);
}
