import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import dao.ArticleDao;
import dao.ArticleDao.Article;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.util.Tuple2;
import io.datakernel.writer.ByteBufWriter;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;

public final class BlogLauncher extends HttpServerLauncher {
	public static final String USE_MYSQL_MODULE_PROP = "useMySqlModule";
	public static final String BASIC_AUTH_REALM = "blog";

	// FOR DEMO PURPOSES ONLY!
	private static final String LOGIN = "admin";
	private static final String PASSWORD = "admin";

	private static ByteBuf applyTemplate(Mustache mustache, @Nullable Object scope) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scope);
		return writer.getBuf();
	}

	@Override
	protected Module getBusinessLogicModule() {
		String property = System.getProperty(USE_MYSQL_MODULE_PROP);
		boolean useMysql = property != null && (property.isEmpty() || Boolean.parseBoolean(property));
		if (useMysql) {
			logger.info("Using MySql-based module");
			return new MySqlModule();
		} else {
			logger.info("Using OT-based module");
			return new OTModule();
		}
	}

	@Provides
	Executor executor() {
		return Executors.newCachedThreadPool();
	}

	@Provides
	MustacheFactory mustacheFactory() {
		return new DefaultMustacheFactory();
	}

	@Provides
	AsyncServlet servlet(ArticleDao articleDao, MustacheFactory mustacheFactory) {
		Mustache indexView = mustacheFactory.compile("templates/index.html");

		Mustache adminView = mustacheFactory.compile("templates/admin.html");
		Mustache createArticleView = mustacheFactory.compile("templates/create.html");
		Mustache editArticleView = mustacheFactory.compile("templates/update.html");

		AsyncServletDecorator basicAuthDecorator = BasicAuth.decorator(BASIC_AUTH_REALM, (login, password) ->
				Promise.of(LOGIN.equals(login) && PASSWORD.equals(password)));

		return RoutingServlet.create()
				// public route
				.map(GET, "/", request -> articleDao.getAllArticles()
						.map(articles -> HttpResponse.ok200()
								.withBody(applyTemplate(indexView, articles.values()))))

				// private routes
				.map("/*", basicAuthDecorator.serve(RoutingServlet.create()
						// views
						.map(GET, "/admin", request -> articleDao.getAllArticles()
								.map(articles -> HttpResponse.ok200()
										.withBody(applyTemplate(adminView, articles.entrySet()))))
						.map(GET, "/create", request -> Promise.of(HttpResponse.ok200()
								.withBody(applyTemplate(createArticleView, null))))
						.map(GET, "/edit/:articleId", request -> {
							String articleIdString = request.getPathParameter("articleId");
							try {
								Long articleId = Long.valueOf(articleIdString);
								return articleDao.getArticle(articleId)
										.then(article -> {
											if (article == null) {
												return Promise.<HttpResponse>ofException(HttpException.notFound404());
											}
											return Promise.of(HttpResponse.ok200()
													.withBody(applyTemplate(editArticleView, new Tuple2<>(articleId, article))));
										});
							} catch (NumberFormatException e) {
								return Promise.of(HttpResponse.ofCode(400).withPlainText("Invalid article id"));
							}
						})

						// api
						.map(POST, "/add", loadBody()
								.serve(request -> {
									String title = request.getPostParameter("title");
									String text = request.getPostParameter("text");
									if (title == null || text == null) {
										return Promise.of(HttpResponse.ofCode(400));
									}
									return articleDao.addArticle(new Article(title, text))
											.map($ -> HttpResponse.redirect302("/admin"));
								}))
						.map(POST, "/update/:articleId", request -> {
							String articleIdString = request.getPathParameter("articleId");
							String title = request.getPostParameter("title");
							String text = request.getPostParameter("text");
							if (title == null || text == null) {
								return Promise.of(HttpResponse.ofCode(400));
							}
							try {
								Long articleId = Long.valueOf(articleIdString);
								return articleDao.updateArticle(articleId, title, text)
										.map($ -> HttpResponse.redirect302("/admin"));
							} catch (NumberFormatException e) {
								return Promise.of(HttpResponse.ofCode(400).withPlainText("Invalid article id"));
							}
						})
						.map(GET, "/delete/:articleId", request -> {
							String articleIdString = request.getPathParameter("articleId");
							try {
								return articleDao.deleteArticle(Long.valueOf(articleIdString))
										.map($ -> HttpResponse.redirect302("/admin"));
							} catch (NumberFormatException e) {
								return Promise.of(HttpResponse.ofCode(400).withPlainText("Invalid article id"));
							}
						}))
				);
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlogLauncher();
		launcher.launch(args);
	}
}
