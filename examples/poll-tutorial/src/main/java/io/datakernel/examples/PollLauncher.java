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
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.writer.ByteBufWriter;

import java.util.Collection;
import java.util.Map;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.map;
import static java.lang.Boolean.parseBoolean;

//[START EXAMPLE]
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
				Mustache singlePollView = new DefaultMustacheFactory().compile("templates/singlePollView.html");
				Mustache singlePollCreate = new DefaultMustacheFactory().compile("templates/singlePollCreate.html");
				Mustache listPolls = new DefaultMustacheFactory().compile("templates/listPolls.html");

				return RoutingServlet.create()
						//[START REGION_1]
						.with(GET, "/", request -> Promise.of(HttpResponse.ok200()
								.withBody(applyTemplate(listPolls, map("polls", pollDao.findAll().entrySet())))))
						//[END REGION_1]
						//[START REGION_2]
						.with(GET, "/poll/:id", request -> {
							String idString = request.getPathParameter("id");
							if (idString == null) {
								return Promise.of(HttpResponse.redirect302("/"));
							}
							int id = Integer.parseInt(idString);
							return Promise.of(HttpResponse.ok200()
									.withBody(applyTemplate(singlePollView, map("id", id, "poll", pollDao.find(id)))));
						})
						//[END REGION_2]
						.with(GET, "/create", request -> Promise.of(HttpResponse.ok200()
								.withBody(applyTemplate(singlePollCreate, ImmutableMap.of()))))
						.with(POST, "/vote", loadBody()
								.serve(request -> {
									Map<String, String> params = request.getPostParameters();
									String option = params.get("option");
									String stringId = params.get("id");
									if (option ==null || stringId == null) {
										return Promise.of(HttpResponse.ofCode(401));
									}

									int id = Integer.parseInt(stringId);
									PollDao.Poll question = pollDao.find(id);

									question.vote(option);

									String referer = request.getHeader(REFERER);
									return Promise.of(HttpResponse.redirect302(referer != null ? referer : "/"));
								}))
						.with(POST, "/add", loadBody()
								.serve(request -> {
									Map<String, String> params = request.getPostParameters();
									String title = params.get("title");
									String message = params.get("message");

									String option1 = params.get("option1");
									String option2 = params.get("option2");

									int id = pollDao.add(new PollDao.Poll(title, message, list(option1, option2)));
									return Promise.of(HttpResponse.redirect302("poll/" + id));
								}))
						.with(POST, "/delete", loadBody()
								.serve(request -> {
									Map<String, String> params = request.getPostParameters();
									String id = params.get("id");
									if (id == null) {
										return Promise.of(HttpResponse.ofCode(401));
									}
									pollDao.remove(Integer.parseInt(id));

									return Promise.of(HttpResponse.redirect302("/"));
								}));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new PollLauncher();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
//[END EXAMPLE]
