package io.global.pixel.dao;

import io.datakernel.async.Promise;
import io.datakernel.http.session.SessionStore;
import io.global.common.KeyPair;
import io.global.pixel.ot.Album;
import io.global.pixel.ot.UserId;
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
}
