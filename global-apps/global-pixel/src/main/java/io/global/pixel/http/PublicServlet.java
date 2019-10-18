package io.global.pixel.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.*;
import io.datakernel.http.session.SessionStore;
import io.datakernel.util.MemSize;
import io.datakernel.util.Tuple1;
import io.datakernel.util.Tuple2;
import io.datakernel.util.Tuple3;
import io.global.appstore.AppStore;
import io.global.common.PubKey;
import io.global.mustache.MustacheTemplater;
import io.global.pixel.container.GlobalPixelContainer;
import io.global.pixel.dao.AlbumDao;
import io.global.pixel.dao.MainDao;
import io.global.pixel.http.view.AlbumView;
import io.global.pixel.ot.Photo;
import io.global.pixel.ot.UserId;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpHeaders.HOST;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.MemSize.kilobytes;
import static io.global.Utils.generateString;
import static io.global.pixel.dao.AlbumDao.ALBUM_NOT_FOUND_EXCEPTION;
import static io.global.pixel.dao.AlbumDao.ROOT_ALBUM;
import static io.global.pixel.dao.AlbumDaoImpl.PHOTO_NOT_FOUND_EXCEPTION;
import static io.global.pixel.http.PhotoDataHandler.createMultipartHandler;
import static io.global.pixel.ot.AuthService.DK_APP_STORE;
import static io.global.pixel.util.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;

public final class PublicServlet {
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";
	private static final String SESSION_ID = "PIXEL_SID";
	private static final MemSize BODY_LIMIT = kilobytes(256);
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
							templater.render("login", map("origin", origin));
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
										.map($ -> redirect302("/")
												.withCookie(HttpCookie.of(SESSION_ID, sessionId)
														.withPath("/")
														.withMaxAge(sessionStore.getSessionLifetime())));
							});
				})
				.map(POST, "/logout", request -> {
					String sessionId = request.getCookie(SESSION_ID);
					if (sessionId == null) {
						return Promise.of(HttpResponse.ok200());
					}
					MainDao commDao = request.getAttachment(MainDao.class);
					return commDao.getSessionStore().remove(sessionId)
							.map($ -> {
								String pk = commDao.getKeys().getPubKey().asString();
								return redirectToReferer(request, "/" + pk)
										.withCookie(HttpCookie.of(SESSION_ID, "<unset>")
												.withPath(pk)
												.withMaxAge(Duration.ZERO));
							});
				});
	}

	private static AsyncServlet ownerServlet(MustacheTemplater templater) {
		return RoutingServlet.create()
				.map(GET, "/", loadBody(BODY_LIMIT)
						.serve(request -> {
							MainDao mainDao = request.getAttachment(MainDao.class);
							return mainDao.getAlbum(ROOT_ALBUM).then(rootAlbum -> {
								AlbumView albumView = AlbumView.from(ROOT_ALBUM, rootAlbum);
								return templater.render("photoList",
										map("album", albumView,
												"allAlbums", getAllAlbums(mainDao),
												"rootAlbum", true,
												"photos", true));
							});
						}))
				.map(GET, "/:page/:size/", loadBody(BODY_LIMIT)
						.serve(request -> {
							MainDao mainDao = request.getAttachment(MainDao.class);
							Tuple2<Integer, Integer> params = PAGINATION_DECODER.decodeOrNull(request);
							if (params == null) {
								params = new Tuple2<>(0, Integer.MAX_VALUE);
							}
							Tuple2<Integer, Integer> finalParams = params;
							return mainDao.getAlbum(ROOT_ALBUM).then(rootAlbum -> {
								AlbumView albumView = AlbumView.from(ROOT_ALBUM, rootAlbum, finalParams.getValue1(), finalParams.getValue2());
								return templater.render("photoList",
										map("album", albumView,
												"allAlbums", getAllAlbums(mainDao),
												"rootAlbum", true,
												"photos", true));
							});

						}))
				.map(GET, "/albums/", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					return mainDao.getAlbums()
							.map(AlbumView::from)
							.then(albumViews -> {
								albumViews.remove(ROOT_ALBUM);
								return templater.render("albumList",
										map("albumList", albumViews.entrySet(), "albums", true));
							});
				})
				.map(GET, "/albums/:page/:size/", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					Tuple2<Integer, Integer> params = PAGINATION_DECODER.decodeOrNull(request);
					if (params == null) {
						params = new Tuple2<>(0, Integer.MAX_VALUE);
					}
					Tuple2<Integer, Integer> finalParams = params;
					return mainDao.getAlbums(finalParams.getValue1(), finalParams.getValue2())
							.map(AlbumView::from)
							.then(albumViews -> {
								albumViews.remove(ROOT_ALBUM);
								return templater.render("albumList",
										map("albumList", albumViews.entrySet(), "albums", true));
							});
				})
				.map(GET, "/new/:albumId", request -> templater.render("uploadPhotos",
						map("albumId", request.getPathParameter("albumId"))))
				.map(POST, "/update/:albumId", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								String albumId = request.getPathParameter("albumId");
								Tuple2<String, String> titleAndDescription = JsonUtils.fromJson(UPDATE_ALBUM_METADATA, request.getBody().asString(UTF_8));
								return mainDao.updateAlbum(albumId,
										titleAndDescription.getValue1(),
										titleAndDescription.getValue2())
										.map($ -> redirectToReferer(request, "/" + (albumId.equals(ROOT_ALBUM) ? "" : albumId)));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/:albumId", request -> {
					MainDao mainDao = request.getAttachment(MainDao.class);
					String albumId = request.getPathParameter("albumId");
					AlbumDao albumDao = mainDao.getAlbumDao(albumId);
					if (albumDao == null) {
						return Promise.ofException(ALBUM_NOT_FOUND_EXCEPTION);
					}
					return albumDao.getAlbum()
							.then(album -> templater.render("photoList",
									map("album", AlbumView.of(albumId, album), "showBar", true)));
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
								Set<String> photoIds = album.getValue3();
								return photoIds.isEmpty() ?
										Promise.ofException(new ParseException(PublicServlet.class, "'photo id' cannot be empty")) :
										validate(title, 120, "Title", true)
												.then($ -> mainDao.generateAlbumId()
														.then(aid -> mainDao.crateAlbum(aid, title, album.getValue2())
																.then($1 -> mainDao.movePhotos(ROOT_ALBUM, aid, photoIds))))
												.map($1 -> redirect302("/albums/"));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(POST, "/move/photo/", loadBody(BODY_LIMIT)
						.serve(request -> {
							try {
								MainDao mainDao = request.getAttachment(MainDao.class);
								Tuple2<String, Set<String>> params = JsonUtils.fromJson(MOVE_PHOTOS_CODEC, request.getBody().asString(UTF_8));
								return mainDao.movePhotos(ROOT_ALBUM, params.getValue1(), params.getValue2())
										.map($ -> redirect302("/" + params.getValue2()));
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
					Map<String, Photo> photoMap = new HashMap<>();
					return PhotoDataHandler.handle(request, createMultipartHandler(albumDao, emptyMap(), photoMap))
							.then($ -> photoMap.isEmpty() ?
									Promise.ofException(new ParseException(PublicServlet.class, "'image_attachment' POST parameter is required")) :
									Promise.complete())
							.thenEx(($, e) -> e == null ?
									Promise.of(redirect302("/" + (albumId.equals(ROOT_ALBUM) ? "" : albumId))) :
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
								Tuple1<String> description = JsonUtils.fromJson(PHOTO_DESCRIPTION_CODEC, request.getBody().asString(UTF_8));
								String descriptionValue = description.getValue1();
								return validate(descriptionValue, 1024, "description", true)
										.then($ -> albumDao.updatePhoto(photoId, description.getValue1())
												.map($1 -> HttpResponse.ok200()));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/download/:thumbnail/:albumId/:photoId", request -> {
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
				})
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
			GlobalPixelContainer container = request.getAttachment(GlobalPixelContainer.class);
			MainDao mainDao = container.getMainDao();
			request.attach(MainDao.class, mainDao);
			String host = request.getHeader(HOST);
			if (host == null) {
				host = request.getHostAndPort();
			}
			assert host != null : "host should not be null here";

			templater.clear();
			templater.put("appStoreUrl", appStoreUrl);
			templater.put("url", host + request.getPathAndQuery());
			templater.put("url.host", host);
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
									return sessionStore.getSessionLifetime();
								}
								return Duration.ZERO;
							})
							.then(maxAge -> servlet.serve(request)
									.map(response -> response.getCookie(SESSION_ID) == null ? // servlet itself had set the session (logout request)
											response.withCookie(HttpCookie.of(SESSION_ID, sessionId)
													.withMaxAge(maxAge)
													.withPath("/")) :
											response));
				};
	}

	private static Promise<Void> validate(String param, int maxLength, String paramName, boolean required) {
		if (param == null && required || (param != null && param.matches(WHITESPACE) && required)) {
			return Promise.ofException(new ParseException(PublicServlet.class, "'" + paramName + "' POST parameter is required"));
		}
		return param != null && param.length() > maxLength ?
				Promise.ofException(new ParseException(PublicServlet.class, paramName + " is too long (" + param.length() + ">" + maxLength + ")")) :
				Promise.complete();
	}
}
