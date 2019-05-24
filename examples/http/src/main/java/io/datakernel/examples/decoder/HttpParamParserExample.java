package io.datakernel.examples.decoder;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.SingleResourceStaticServlet;
import io.datakernel.http.parser.HttpParamParser;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncServletWrapper.loadBody;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.parser.HttpParamParsers.ofPost;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class HttpParamParserExample extends HttpServerLauncher {
	private final static HttpParamParser<Role> roleDecoder = HttpParamParser.create(Role::new,
			ofPost("title", "User"));

	private final static HttpParamParser<User> userDecoder = HttpParamParser.create(User::new,
			ofPost("name").validate(name -> !name.isEmpty()),
			ofPost("age", Double::valueOf, null).map(Double::intValue),
			roleDecoder).withId("");

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet mainServlet(Eventloop eventloop) {
				return RoutingServlet.create()
						.with("/", SingleResourceStaticServlet.create(eventloop, ofClassPath(newCachedThreadPool()),
								"static/decoder/decoderExample.html"))
						.with(POST, "/send", loadBody()
								.then(request -> {
									User decodedUser = decodedUser = userDecoder.parseOrNull(request);

									if (decodedUser == null) {
										return Promise.of(HttpResponse.redirect302("/"));
									}
									return Promise.of(HttpResponse.ok200()
											.withBody(wrapUtf8("<a href=\"/\">" +
													decodedUser.toString() +
													"</a>")));
								}));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpParamParserExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
