package io.datakernel.examples;

import com.google.common.base.Charsets;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.di.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;
import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

//[START EXAMPLE]
public final class TodoListLauncher extends HttpServerLauncher {
	//[START REGION_1]
	private static final StructuredCodec<Plan> PLAN_CODEC = object(Plan::new,
			"text", Plan::getText, STRING_CODEC,
			"isComplete", Plan::isComplete, BOOLEAN_CODEC);

	private static final StructuredCodec<Record> RECORD_CODEC = object(Record::new,
			"title", Record::getTitle, STRING_CODEC,
			"plans", Record::getPlans, ofList(PLAN_CODEC));
	//[END REGION_1]

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {

			@Provides
			RecordDAO recordRepo() {
				return new RecordImplDAO();
			}

			@Provides
			@Named("static")
			AsyncServlet servlet(Eventloop eventloop) {
				return StaticServlet.create(eventloop, ofClassPath(newCachedThreadPool(), "build/"));
			}

			@Provides
			AsyncServlet servlet(RecordDAO recordDAO, @Named("static") AsyncServlet staticServlet) {
				return RoutingServlet.create()
						//[START REGION_2]
						.with("/*", staticServlet)
						//[END REGION_2]
						.with(POST, "/add", request -> request.getBody()
								.then(body -> {
									try {
										Record record = JsonUtils.fromJson(RECORD_CODEC, body.getString(Charsets.UTF_8));
										recordDAO.add(record);

										return Promise.of(HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.of(HttpResponse.ofCode(400));
									} finally {
										body.recycle();
									}
								}))
						.with(GET, "/get/all", request -> {
							Map<Integer, Record> records = recordDAO.findAll();
							return Promise.of(HttpResponse.ok200()
									.withJson(ofMap(INT_CODEC, RECORD_CODEC), records));
						})
						//[START REGION_3]
						.with(GET, "/delete/:recordId", request -> {
							String stringId = request.getPathParameter("recordId");
							int id = parseInt(requireNonNull(stringId));
							recordDAO.delete(id);
							return Promise.of(HttpResponse.ok200());
						})
						//[END REGION_3]
						.with(GET, "/toggle/:recordId/:planId", request -> {
							String stringId = request.getPathParameter("recordId");
							String stringPlanId = request.getPathParameter("planId");
							int id = parseInt(requireNonNull(stringId));
							int planId = parseInt(requireNonNull(stringPlanId));

							Record record = recordDAO.find(id);
							Plan plan = record.getPlans().get(planId);
							plan.toggle();

							return Promise.of(HttpResponse.ok200());
						 });
			}
		});
	}

	//[START REGION_4]
	public static void main(String[] args) throws Exception {
		TodoListLauncher launcher = new TodoListLauncher();
		launcher.launch(args);
	}
	//[END REGION_4]
}
//[END EXAMPLE]
