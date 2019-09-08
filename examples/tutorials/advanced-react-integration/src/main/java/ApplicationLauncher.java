import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.promise.Promise;

import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START REGION_1]
public final class ApplicationLauncher extends HttpServerLauncher {

	private static final StructuredCodec<Plan> PLAN_CODEC = object(Plan::new,
			"text", Plan::getText, STRING_CODEC,
			"isComplete", Plan::isComplete, BOOLEAN_CODEC);

	private static final StructuredCodec<Record> RECORD_CODEC = object(Record::new,
			"title", Record::getTitle, STRING_CODEC,
			"plans", Record::getPlans, ofList(PLAN_CODEC));

	@Provides
	RecordDAO recordRepo() {
		return new RecordImplDAO();
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	AsyncServlet servlet(Executor executor, RecordDAO recordDAO) {
		return RoutingServlet.create()
				.map("/*", StaticServlet.ofClassPath(executor, "build/")
						.withIndexHtml())
				//[END REGION_2]
				//[START REGION_3]
				.map(POST, "/add", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								Record record = JsonUtils.fromJson(RECORD_CODEC, body.getString(UTF_8));
								recordDAO.add(record);

								return Promise.of(HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.of(HttpResponse.ofCode(400));
							}
						}))
				.map(GET, "/get/all", request -> {
					Map<Integer, Record> records = recordDAO.findAll();
					return Promise.of(
							HttpResponse.ok200()
									.withJson(ofMap(INT_CODEC, RECORD_CODEC), records));
				})
				//[START REGION_4]
				.map(GET, "/delete/:recordId", request -> {
					int id = parseInt(request.getPathParameter("recordId"));
					recordDAO.delete(id);
					return Promise.of(HttpResponse.ok200());
				})
				//[END REGION_4]
				.map(GET, "/toggle/:recordId/:planId", request -> {
					int id = parseInt(request.getPathParameter("recordId"));
					int planId = parseInt(request.getPathParameter("planId"));

					Record record = recordDAO.find(id);
					Plan plan = record.getPlans().get(planId);
					plan.toggle();

					return Promise.of(HttpResponse.ok200());
				});
		//[END REGION_3]
	}

	//[START REGION_5]
	public static void main(String[] args) throws Exception {
		ApplicationLauncher launcher = new ApplicationLauncher();
		launcher.launch(args);
	}
	//[END REGION_5]
}
