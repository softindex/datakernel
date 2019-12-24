package io.global.photos.http;

import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.tuple.Tuple1;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.appstore.AppStore;
import io.global.common.PubKey;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.dao.AlbumDao;
import io.global.photos.dao.MainDao;
import io.global.photos.util.ViewUtil;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.datakernel.common.MemSize.kilobytes;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.global.Utils.*;
import static io.global.ot.session.AuthService.DK_APP_STORE;
import static io.global.photos.dao.AlbumDao.ALBUM_NOT_FOUND_EXCEPTION;
import static io.global.photos.dao.AlbumDao.ROOT_ALBUM;
import static io.global.photos.dao.AlbumDaoImpl.PHOTO_NOT_FOUND_EXCEPTION;
import static io.global.photos.http.PhotoDataHandler.createMultipartHandler;
import static io.global.photos.util.Utils.*;
import static io.global.photos.util.ViewUtil.albumViewFrom;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;

public final class PublicServlet {
	private static final Throwable IMAGE_PARAMS_EXCEPTION = new StacklessException(PublicServlet.class, "'image_attachment' POST parameter is required");
	private static final MemSize BODY_LIMIT = kilobytes(256);
	private static final String SESSION_ID = "PHOTOS_SID";
	private static final int LIMIT_ITEMS_PER_PAGE = 1000;
	private static final int MIN_LIMIT_ITEMS_PER_PAGE = 50;
	private static boolean isChanged = true;
	private static Set<Tuple2<String, String>> allAlbums;

	public static AsyncServlet create(String appStoreUrl, AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map("/*", ownerServlet(templater))
				.map("/auth/*", authServlet(appStore, templater))
				.then(session(templater))
				.then(setup(appStoreUrl, templater));
	}

	private static AsyncServlet authServlet(AppStore appStore, MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/login", request -> {
					String origin = request.getQueryParameter("origin");
					return request.getAttachment(UserId.class) != null ?
							Promise.of(redirectToReferer(request, "/")) :
							templater.render("login", map("origin", origin), isGzipAccepted(request));
				})
				.map(GET, "/authorize", request -> {
					String token = request.getQueryParameter("token");
					if (token == null) {
						return Promise.ofException(HttpException.ofCode(400));
					}
					return appStore.exchangeAuthToken(token)
							.then(profile -> {
								MainDao mainDao = request.getAttachment(MainDao.class);
								PubKey containerPubKey = mainDao.getKeys().getPubKey();
								PubKey pubKey = profile.getPubKey();
								if (!containerPubKey.equals(pubKey)) {
									return Promise.<HttpResponse>ofException(NOT_PRIVILEGED);
								}
								UserId userId = new UserId(DK_APP_STORE, pubKey.asString());
								String sessionId = generateString(32);
								SessionStore<UserId> sessionStore = mainDao.getSessionStore();
								return sessionStore.save(sessionId, userId)
										.map($ -> {
											HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
													.withPath("/");
											Duration lifetimeHint = sessionStore.getSessionLifetimeHint();
											if (lifetimeHint != null) {
												sessionCookie.setMaxAge(lifetimeHint);
											}
											return redirect302("/")
													.withCookie(sessionCookie);
										});
							});
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					MainDao commDao = request.getAttachment(MainDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> commDao.getKeys().getPubKey().asString())
							.map(pk -> {
								HttpCookie cookie = HttpCookie.of(SESSION_ID, "<unset>")
										.withPath(pk)
										.withMaxAge(Duration.ZERO);
								return redirectToReferer(request, "/" + pk)
										.withCookie(cookie);
							});
				});
	}

	private static AsyncServlet ownerServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", loadBody(BODY_LIMIT).serve(request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					Tuple2<Integer, Integer> params = PAGINATION_DECODER.decodeOrNull(request);
					return mainDao.getAmountPhotos(ROOT_ALBUM).then(amount -> {
						if (params == null || params.getValue2() > LIMIT_ITEMS_PER_PAGE) {
							return Promise.of(redirect302("/?page=1&size=" + MIN_LIMIT_ITEMS_PER_PAGE));
						}
						return mainDao.getAlbum(ROOT_ALBUM)
								.map(rootAlbum -> albumViewFrom(ROOT_ALBUM, rootAlbum, params.getValue1() - 1, params.getValue2(), mainDao::getBase64))
								.then(albumView -> templater.render("photoList",
										map("album", albumView,
												"amountItems", amount,
												"allAlbums", getAllAlbums(mainDao),
												"rootAlbum", true,
												"photos", true),
										isGzipAccepted(request)));
					});
				}))
				.map(GET, "/albums/", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					Tuple2<Integer, Integer> params = PAGINATION_DECODER.decodeOrNull(request);
					return mainDao.getAlbumsAmount()
							.then(amount -> {
								if (params == null || params.getValue2() > LIMIT_ITEMS_PER_PAGE) {
									return Promise.of(redirect302("/albums/?page=1&size=" + MIN_LIMIT_ITEMS_PER_PAGE));
								}
								return mainDao.getAlbums(params.getValue1() - 1, params.getValue2())
										.then(ViewUtil::albumViewFrom)
										.then(albumViews -> templater.render("albumList",
												map("albumList", albumViews.entrySet(),
														"amountItems", amount,
														"albums", true),
												isGzipAccepted(request)));
							});
				})
				.map(GET, "/new/:albumId", request -> templater.render("uploadPhotos",
						map("albumId", request.getPathParameter("albumId")),
						isGzipAccepted(request)))
				.map(POST, "/update/:albumId", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								String albumId = request.getPathParameter("albumId");
								if (albumId.equals(ROOT_ALBUM)) {
									return Promise.of(redirectToReferer(request, "/"));
								}
								String body = request.getBody().asString(UTF_8);
								Tuple2<String, String> titleAndDescription = JsonUtils.fromJson(UPDATE_ALBUM_METADATA, body);
								return mainDao.updateAlbum(albumId, titleAndDescription.getValue1(), titleAndDescription.getValue2())
										.map($ -> redirectToReferer(request, "/" + albumId));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/:albumId", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					String albumId = request.getPathParameter("albumId");
					if (albumId.equals(ROOT_ALBUM)) {
						return Promise.of(redirect302("/"));
					}
					Tuple2<Integer, Integer> params = PAGINATION_DECODER.decodeOrNull(request);
					return mainDao.getAmountPhotos(albumId)
							.then(amount -> {
								if (params == null || params.getValue2() > LIMIT_ITEMS_PER_PAGE) {
									return Promise.of(redirect302(request.getPath() + "/?page=1&size=" + MIN_LIMIT_ITEMS_PER_PAGE));
								}
								AlbumDao albumDao = mainDao.getAlbumDao(albumId);
								if (albumDao == null) {
									return Promise.<HttpResponse>ofException(ALBUM_NOT_FOUND_EXCEPTION);
								}
								return albumDao.getAlbum()
										.map(album -> albumViewFrom(albumId, album, params.getValue1() - 1, params.getValue2(), mainDao::getBase64))
										.then(albumView -> templater.render("photoList",
												map("album", albumView, "amountItems", amount, "showBar", true),
												isGzipAccepted(request)));
							});
				})
				.map(POST, "/delete/:albumId", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					String albumId = request.getPathParameter("albumId");
					return albumId.equals(ROOT_ALBUM) ?
							Promise.ofException(new StacklessException("Cannot delete root album")) :
							mainDao.removeAlbum(albumId)
									.map($ -> redirect302("/"));
				})
				.map(POST, "/new/album", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								Tuple3<String, String, Set<String>> album = JsonUtils.fromJson(ALBUM_CODEC, request.getBody().asString(UTF_8));
								String title = album.getValue1();
								String description = album.getValue2();
								Set<String> photoIds = album.getValue3();
								return photoIds.isEmpty() ?
										Promise.ofException(new ParseException(PublicServlet.class, "'photo id' cannot be empty")) :
										validate(title, 120, "Title", true)
												.then($ -> validate(description, 1024, "Description", false))
												.then($ -> mainDao.generateAlbumId())
												.then(aid -> mainDao.crateAlbum(aid, title, description)
														.map($ -> aid))
												.then(aid -> mainDao.movePhotos(ROOT_ALBUM, aid, photoIds))
												.map($ -> redirect302("/albums/"));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(POST, "/move/photo/", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								String body = request.getBody().asString(UTF_8);
								Tuple2<String, Set<String>> params = JsonUtils.fromJson(MOVE_PHOTOS_CODEC, body);
								return mainDao.movePhotos(ROOT_ALBUM, params.getValue1(), params.getValue2())
										.map($ -> redirect302("/"));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(POST, "/upload/photos/:albumId", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					String albumId = request.getPathParameter("albumId");
					AlbumDao albumDao = mainDao.getAlbumDao(albumId);
					if (albumDao == null) {
						return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
					}
					Map<String, String> photoMap = new HashMap<>();
					return PhotoDataHandler.handle(request, createMultipartHandler(albumDao, emptyMap(), photoMap))
							.then($ -> photoMap.isEmpty() ?
									Promise.ofException(IMAGE_PARAMS_EXCEPTION) :
									Promise.complete())
							.thenEx(($, e) -> e == null ?
									Promise.of(redirect302(albumId.equals(ROOT_ALBUM) ? "/" : "/" + albumId)) :
									Promise.ofException(HttpException.ofCode(403, e.getMessage())));
				})
				.map(POST, "/delete/photo/:albumId", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								String albumId = request.getPathParameter("albumId");
								AlbumDao albumDao = mainDao.getAlbumDao(albumId);
								if (albumDao == null) {
									return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
								}
								Set<String> photoIds = JsonUtils.fromJson(SET_CODEC, request.getBody().asString(UTF_8));
								return photoIds.isEmpty() ?
										Promise.ofException(PHOTO_NOT_FOUND_EXCEPTION) :
										albumDao.removePhotos(photoIds)
												.map($ -> redirect302("/" + albumId));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(POST, "/update/:albumId/:photoId", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								String albumId = request.getPathParameter("albumId");
								String photoId = request.getPathParameter("photoId");
								AlbumDao albumDao = mainDao.getAlbumDao(albumId);
								if (albumDao == null) {
									return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
								}
								String body = request.getBody().asString(UTF_8);
								Tuple1<String> description = JsonUtils.fromJson(PHOTO_DESCRIPTION_CODEC, body);
								String descriptionValue = description.getValue1();
								return validate(descriptionValue, 1024, "description", true)
										.then($ -> albumDao.updatePhoto(photoId, descriptionValue))
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/download/:thumbnail/:albumId/:photoId", cachedContent().serve(request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					String albumId = request.getPathParameter("albumId");
					String photoID = request.getPathParameter("photoId");
					String thumbnail = request.getPathParameter("thumbnail");
					AlbumDao albumDao = mainDao.getAlbumDao(albumId);

					if (albumDao == null) {
						return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
					}
					return albumDao.getPhoto(photoID)
							.then(photo -> albumDao.photoSize(thumbnail, photoID)
									.thenEx((size, e) -> {
										if (e == null && photo != null) {
											return HttpResponse.file(
													(offset, limit) -> albumDao.loadPhoto(thumbnail, photoID, offset, limit),
													photo.getFilename(),
													size,
													request.getHeader(HttpHeaders.RANGE)
											);
										} else if (e == PHOTO_NOT_FOUND_EXCEPTION) {
											return Promise.of(HttpResponse.notFound404());
										} else if (e != null) {
											return Promise.<HttpResponse>ofException(e);
										} else {
											return Promise.of(HttpResponse.notFound404());
										}
									}));
				}))
				.then(servlet ->
						request -> {
							UserId userId = request.getAttachment(UserId.class);
							if (userId == null) return Promise.of(redirect302("/auth/login"));
							if (request.getMethod() == POST) isChanged = true;
							return servlet.serve(request);
						});
	}

	private static Promise<Set<Tuple2<String, String>>> getAllAlbums(MainDao mainDao) {
		return (isChanged || allAlbums == null) ?
				mainDao.getAlbums().map(albums -> {
					Set<Tuple2<String, String>> all = albums.entrySet()
							.stream()
							.map(entry -> new Tuple2<>(entry.getKey(), entry.getValue().getTitle()))
							.collect(Collectors.toSet());
					all.removeIf(tuple -> tuple.getValue1().equals(ROOT_ALBUM));
					return all;
				}).whenResult(res -> {
					allAlbums = res;
					isChanged = false;
				}) :
				Promise.of(allAlbums);
	}

	private static AsyncServletDecorator setup(String appStoreUrl, MustacheTemplater templater) {
		return onRequest(request -> {
			GlobalPhotosContainer container = request.getAttachment(GlobalPhotosContainer.class);
			MainDao mainDao = container.getMainDao();
			request.attach(MainDao.class, mainDao);
			String host = request.getHeader(HOST);
			if (host == null) {
				host = request.getHostAndPort();
			}
			assert host != null : "host should not be null here";

			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);
		});
	}

	private static AsyncServletDecorator session(MustacheTemplater templater) {
		return servlet ->
				request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return servlet.serve(request);
					}
					MainDao mainDao = request.getAttachment(MainDao.class);
					SessionStore<UserId> sessionStore = mainDao.getSessionStore();
					return sessionStore.get(sessionId)
							.map(userId -> {
								if (userId != null) {
									request.attach(UserId.class, userId);
									templater.put("privileged", true);
									return sessionStore.getSessionLifetimeHint();
								}
								return Duration.ZERO;
							})
							.then(maxAge -> servlet.serveAsync(request)
									.map(response -> {
										// servlet itself had set the session (logout request)
										if (response.getCookie(SESSION_ID) != null) {
											return response;
										}
										HttpCookie sessionCookie = HttpCookie.of(SESSION_ID, sessionId)
												.withPath("/");
										if (maxAge != null) {
											sessionCookie.setMaxAge(maxAge);
										}
										return response.withCookie(sessionCookie);
									}));
				};
	}
}
