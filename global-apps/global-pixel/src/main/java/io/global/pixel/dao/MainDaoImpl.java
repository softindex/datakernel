package io.global.pixel.dao;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.http.session.SessionStore;
import io.datakernel.ot.OTStateManager;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.pixel.container.GlobalPixelContainer;
import io.global.pixel.ot.Album;
import io.global.pixel.ot.AlbumOtState;
import io.global.pixel.ot.UserId;
import io.global.pixel.ot.operation.AlbumAddOperation;
import io.global.pixel.ot.operation.AlbumAddPhotoOperation;
import io.global.pixel.ot.operation.AlbumChangeOperation;
import io.global.pixel.ot.operation.AlbumOperation;
import io.global.pixel.util.Utils;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static io.global.pixel.dao.AlbumDao.ALBUM_ALREADY_EXIST;
import static io.global.pixel.dao.AlbumDao.ALBUM_NOT_FOUND_EXCEPTION;
import static java.lang.Integer.MAX_VALUE;

public class MainDaoImpl implements MainDao {
	private final OTStateManager<CommitId, AlbumOperation> mainStateManager;
	private final Map<String, AlbumDao> albumDaoMap = new HashMap<>();
	private final Map<String, Album> albumView;
	private final GlobalPixelContainer container;
	private final AlbumDaoImpl.Builder albumDaoBuilder;
	private final FsClient fsClient;

	public MainDaoImpl(GlobalPixelContainer container, AlbumDaoImpl.Builder albumDaoBuilder, FsClient fsClient) {
		this.mainStateManager = container.getMainState();
		this.container = container;
		this.albumDaoBuilder = albumDaoBuilder;
		this.fsClient = fsClient;
		albumView = ((AlbumOtState) mainStateManager.getState()).getMap();
	}

	@Nullable
	@Override
	public AlbumDao getAlbumDao(String albumId) {
		Album album = albumView.get(albumId);
		return album != null ? albumDaoMap.computeIfAbsent(albumId,
				$ -> albumDaoBuilder.build(container, album, albumId, fsClient, this)) : null;
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
				.then(res -> fromAlbumDao == null || !res ?
						Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION) :
						Promises.all(photoIds.stream()
								.map(id -> fromAlbumDao.getPhoto(id)
										.whenResult(photo ->
												mainStateManager.add(new AlbumAddPhotoOperation(to, id, photo, false)))))
								.then($ -> mainStateManager.sync()));
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
