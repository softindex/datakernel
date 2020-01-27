package io.global.forum;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.global.comm.http.CommServletModule;

public final class ForumServlet extends AbstractModule {
	private ForumServlet() {
	}

	public static Module create() {
		return CommServletModule.create("FORUM_SID", 5, 10, 10, 2)
				.overrideWith(new ForumServlet());
	}
}
