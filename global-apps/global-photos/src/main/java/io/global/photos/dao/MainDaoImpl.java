package io.global.photos.dao;

import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.di.annotation.Inject;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.session.UserId;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.ot.Album;
import io.global.photos.ot.AlbumOtState;
import io.global.photos.ot.operation.AlbumAddOperation;
import io.global.photos.ot.operation.AlbumAddPhotoOperation;
import io.global.photos.ot.operation.AlbumChangeOperation;
import io.global.photos.ot.operation.AlbumOperation;
import io.global.photos.util.Utils;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static io.global.photos.GlobalPhotosModule.base64Thumbnail;
import static io.global.photos.dao.AlbumDao.*;
import static java.lang.Integer.MAX_VALUE;

public class MainDaoImpl implements MainDao {
	private static final Base64.Encoder ENCODE_BASE_64 = Base64.getEncoder();
	private final OTStateManager<CommitId, AlbumOperation> mainStateManager;
	private final Map<String, AlbumDao> albumDaoMap = new HashMap<>();
	private final Map<String, Album> albumView;
	private GlobalPhotosContainer container;
	@Inject private AlbumDaoImpl.Builder albumDaoBuilder;
	@Inject private FsClient fsClient;

	@Inject
	public MainDaoImpl(OTStateManager<CommitId, AlbumOperation> mainStateManager) {
		this.mainStateManager = mainStateManager;
		albumView = ((AlbumOtState) mainStateManager.getState()).getMap();
	}

	public void setContainer(GlobalPhotosContainer container) {
		this.container = container;
	}

	@Nullable
	@Override
	public AlbumDao getAlbumDao(String albumId) {
		Album album = albumView.get(albumId);
		return album != null ? albumDaoMap.computeIfAbsent(albumId,
				$ -> albumDaoBuilder.build(container, albumView.get(albumId), albumId, fsClient, this)) : null;
	}

	@Override
	public Promise<String> generateAlbumId() {
		String postId;
		do {
			postId = Utils.generateId();
		} while (albumView.containsKey(postId));
		return Promise.of(postId);
	}

	@Override
	public Promise<Album> getAlbum(String aid) {
		return Promise.of(albumView.get(aid));
	}

	@Override
	public Promise<Void> crateAlbum(String aid, @Nullable String title, @Nullable String description) {
		return albumExist(aid)
				.then(res -> {
					if (res) {
						return Promise.ofException(ALBUM_ALREADY_EXIST);
					}
					mainStateManager.add(new AlbumAddOperation(aid, title, description, false));
					return mainStateManager.sync();
				});
	}

	@Override
	public Promise<Void> updateAlbum(String albumId, @Nullable String title, @Nullable String description) {
		return albumExist(albumId)
				.then(res -> {
					if (!res) {
						return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
					}
					mainStateManager.add(new AlbumChangeOperation(albumId, albumView.get(albumId), Album.create(title, description)));
					return mainStateManager.sync();
				});
	}

	@Override
	public Promise<Void> removeAlbum(String albumId) {
		AlbumDao albumDao = getAlbumDao(albumId);
		if (albumDao == null) {
			return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
		}
		Album album = albumView.get(albumId);
		return albumDao.removeAllPhotos()
				.then($ -> {
					mainStateManager.add(new AlbumAddOperation(albumId, album.getTitle(), album.getDescription(), true));
					albumDaoMap.remove(albumId);
					return mainStateManager.sync();
				});
	}

	@Override
	public Promise<Void> movePhotos(String from, String to, Set<String> photoIds) {
		AlbumDao fromAlbumDao = getAlbumDao(from);
		return albumExist(to)
				.then(res -> {
					if (fromAlbumDao == null || !res) {
						return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
					}
					return Promises.all(photoIds.stream()
							.map(id -> fromAlbumDao.getPhoto(id)
									.whenResult(photo ->
											mainStateManager.add(new AlbumAddPhotoOperation(to, id, photo, false)))));
				})
				.then($ -> mainStateManager.sync());
	}

	@Override
	public Promise<Boolean> albumExist(String aid) {
		return Promise.of(albumView.get(aid) != null);
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return container.getSessionStore();
	}

	@Override
	public KeyPair getKeys() {
		return container.getKeys();
	}

	@Override
	public Promise<Integer> getAmountPhotos(String albumId) {
		return getAlbum(albumId)
				.map(album -> album == null ? -1 : album.getPhotoMap().size());
	}

	@Override
	public Promise<Integer> getAlbumsAmount() {
		return getAlbums()
				.map(Map::size);
	}

	@Override
	public Promise<String> getBase64(String photoId) {
		return fsClient.download(ROOT_ALBUM + "/" + base64Thumbnail + "/" + photoId)
				.thenEx((value, e) -> {
					if (e != null) {
						return Promise.ofException(e == FsClient.FILE_NOT_FOUND ? PHOTO_NOT_FOUND_EXCEPTION : e);
					}
					return value.toCollector(ByteBufQueue.collector())
							.then(image -> Promise.ofBlockingCallable(container.getExecutor(),
									() -> {
										byte[] base64Buffer = ENCODE_BASE_64.encode(image.asArray());
										return new String(base64Buffer, StandardCharsets.UTF_8);
									}));
				});
	}

	@Override
	public Promise<Map<String, Album>> getAlbums(Integer page, Integer size) {
		long res = page.longValue() * size.longValue();
		long offset = ((int) res != res) ? MAX_VALUE : res;
		return Promise.of(albumView.entrySet()
				.stream()
				.skip(offset)
				.limit(size)
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
	}

	@Override
	public Promise<Map<String, Album>> getAlbums() {
		return getAlbums(0, MAX_VALUE);
	}
}
