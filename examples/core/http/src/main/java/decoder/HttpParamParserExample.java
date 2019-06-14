package decoder;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.di.annotation.Provides;
import io.datakernel.functional.Either;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.parser.HttpParamParseErrorsTree;
import io.datakernel.http.parser.HttpParamParser;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.writer.ByteBufWriter;

import java.util.Map;

import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.parser.HttpParamParsers.ofPost;
import static io.datakernel.util.CollectionUtils.map;

public final class HttpParamParserExample extends HttpServerLauncher {
	private final static String SEPARATOR = "-";
	private final static HttpParamParser<Address> addressDecoder = HttpParamParser.create(Address::new,
			ofPost("title", "")
					.validate(param -> !param.isEmpty(), "Title cannot be empty"));

	private final static HttpParamParser<Contact> contactDecoder = HttpParamParser.create(Contact::new,
			ofPost("name").validate(name -> !name.isEmpty(), "Name cannot be empty"),
			ofPost("age")
					.map(Double::valueOf, "Cannot parse age")
					.map(Double::intValue)
					.validate(age -> age > 18, "Age must be greater than 18"),
			addressDecoder.withId("contact-address"));

	private static ByteBuf applyTemplate(Mustache mustache, Map<String, Object> scopes) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scopes);
		return writer.getBuf();
	}

	@Provides
	ContactDAO dao() {
		return new ContactDAOImpl();
	}

	@Provides
	AsyncServlet mainServlet(ContactDAO contactDAO) {
		Mustache contactListView = new DefaultMustacheFactory().compile("static/decoder/contactList.html");
		return RoutingServlet.create()
				.with("/", request -> Promise.of(HttpResponse.ok200()
						.withBody(applyTemplate(contactListView, map("contacts", contactDAO.getAll())))))
				.with(POST, "/add", AsyncServletDecorator.loadBody()
						.serve(request -> {
							Either<Contact, HttpParamParseErrorsTree> decodedUser = contactDecoder.parse(request);
							if (decodedUser.isLeft()) {
								contactDAO.add(decodedUser.getLeft());
							}
							Map<String, Object> scopes = map("contacts", contactDAO.getAll());
							if (decodedUser.isRight()) {
								scopes.put("errors", decodedUser.getRight().toMap(SEPARATOR));
							}
							return Promise.of(HttpResponse.ok200()
									.withBody(applyTemplate(contactListView, scopes)));
						}));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpParamParserExample();
		launcher.launch(args);
	}
}
