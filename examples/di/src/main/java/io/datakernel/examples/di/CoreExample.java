package io.datakernel.examples.di;

import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;

/**
 * The core of the datakernel DI has no reflection and is really simple and fast.
 * A module is a collection of bindings and a binding is a recipe of how to make an instance of certain type.
 * You simply define all these recipes with a DSL.
 * <p>
 * - In this example you make a lambda which creates an instance of ConsoleMessageSenderImpl and make it a recipe for it.
 * - Then you say that to create an instance of MessageSender you can use the ConsoleMessageSenderImpl, which is known how to make.
 * - And then you add a recipe with a lambda for an Application instance, which depends on MessageSender, which is known to be made as ConsoleMessageSenderImpl.
 * <p>
 * Keep in mind, that all of those recipes can be split into multiple modules, and you don't need to know an exact implementation of the MessageSender
 * interface, you just know that somewhere somebody provided a recipe for it, - that is the whole point of the Dependency Injection paradigm.
 * <p>
 * One notable difference, that we will dive into more later is the one that all the recipes are by default 'singletons' - meaning that
 * an object is created from the recipe at most once per its key per injector instance, cached and reused for any other recipes that depend on it.
 * <p>
 * Zero reflection, explicit, simple and powerful DSL - this is the core of the datakernel DI.
 */
public final class CoreExample {

	interface MessageSender {

		void send(String message);
	}

	static class ConsoleMessageSenderImpl implements MessageSender {

		@Override
		public void send(String message) {
			System.out.println("received message: " + message);
		}
	}

	static class Application {
		private final MessageSender sender;

		Application(MessageSender sender) {
			this.sender = sender;
		}

		void hello() {
			sender.send("hello from application");
		}
	}

	static class ApplicationModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(ConsoleMessageSenderImpl.class).to(ConsoleMessageSenderImpl::new);

			bind(MessageSender.class).to(ConsoleMessageSenderImpl.class);

			bind(Application.class).to(Application::new, MessageSender.class);
		}
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new ApplicationModule());
		Application application = injector.getInstance(Application.class);

		application.hello();
	}
}
