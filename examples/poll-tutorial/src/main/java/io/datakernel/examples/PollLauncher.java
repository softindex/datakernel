package io.datakernel.examples;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;
import java.util.Map;

import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.map;
import static java.lang.Boolean.parseBoolean;

public final class PollLauncher extends HttpServerLauncher {

	private static ByteBuf applyTemplate(Mustache mustache, Map<String, Object> scopes) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scopes);
		return writer.getBuf();
	}

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			PollDao pollRepo() {
				return new PollDaoImpl();
			}

			@Provides
			@Singleton
			AsyncServlet servlet(PollDao pollDao) {
				Mustache singlePollView = new DefaultMustacheFactory().compile("site/singlePollView.html");
				Mustache singlePollCreate = new DefaultMustacheFactory().compile("site/singlePollCreate.html");
				Mustache listPolls = new DefaultMustacheFactory().compile("site/listPolls.html");

				return RoutingServlet.create()
						.with(GET, "/", request -> Promise.of(HttpResponse.ok200()
								.withBody(applyTemplate(listPolls, map("polls", pollDao.findAll().entrySet())))))
						.with(GET, "/poll/:id", request -> {
							String idString = request.getPathParameterOrNull("id");
							if (idString == null) {
								return Promise.of(HttpResponse.redirect302("/"));
							}
							int id = Integer.parseInt(idString);
							return Promise.of(HttpResponse.ok200()
									.withBody(applyTemplate(singlePollView, map("id", id, "poll", pollDao.find(id)))));
						})
						.with(GET, "/create", request -> Promise.of(HttpResponse.ok200()
								.withBody(applyTemplate(singlePollCreate, ImmutableMap.of()))))
						.with(POST, "/vote", request -> request.getPostParameters()
								.map(params -> {
									String option = params.get("option");
									String stringId = params.get("id");

									int id = Integer.parseInt(stringId);
									PollDao.Poll question = pollDao.find(id);

									question.vote(option);

									String referer = request.getHeaderOrNull(REFERER);
									return HttpResponse.redirect302(referer != null ? referer : "/");
								}))
						.with(POST, "/add", request -> request.getPostParameters()
								.map(params -> {
									String title = params.get("title");
									String message = params.get("message");

									String option1 = params.get("option1");
									String option2 = params.get("option2");

									int id = pollDao.add(new PollDao.Poll(title, message, list(option1, option2)));
									return HttpResponse.redirect302("poll/" + id);
								}))
						.with(POST, "/delete", request -> request.getPostParameters()
								.map(params -> {
									String id = params.get("id");
									pollDao.remove(Integer.parseInt(id));

									return HttpResponse.redirect302("/");
								}));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new PollLauncher();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
