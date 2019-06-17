import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Name;
import io.datakernel.di.module.AbstractModule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

/**
 * Suppose that you really need to have two distinct recipes for two different implementations
 * of the same interface that you cannot really decouple into separate Java types.
 * <p>
 * Any dependency injection framework has the notion of `key` - something that identifies a recipe
 * (lets call them bindings, as they are called similar to any other DI frameworks out there).
 * Usually a key is just a Java type of the object that is made by the binding, but as was said before,
 * there are situations in which you need multiple keys of the same type.
 * <p>
 * So we introduce a concept of name annotation - a key is actually a pair consisting of a returned type
 * and a nullable name annotation.
 * There is also commonly a hardcoded @Named("string parameter") annotation, that allows using strings as names
 * for quick prototyping and so on.
 * Note that in datakernel DI you can make your own annotation with parameters with little to no hacks and tricks, more on that later.
 * <p>
 * Our next example is similar to the previous one except that first - we have two keys for different MessageSenders, annotated with
 * the name annotation we were talking about before, and second - we use some reflection for better and even more consise DSL.
 * <p>
 * - First binding is a simple binding where we even used a shortcut and didn't use a lambda since we already made an instance.
 * - Next, we bind a message sender to its implementation - and note the .annotatedWith DSL call - we specify what key exactly we are binding.
 * Note that we did not describe how to make an ConsoleMessageSenderImpl instance - it has an inject annotation on it which means that
 * a binding that will actually make the instance will be generated automatically and call its default constructor via reflection.
 * - Same as above, but with another syntax and a custom annotation - we make a concrete Key instance and bind it to the implementation.
 * The implementation is using an inject constructor - constructor from which a binding will be made automatically
 * - And lastly, we use a simple bind call without ending for the annotation class - requesting a binding to be made in same way as above.
 * The generated binding will call the selected constructor (and via inject annotation on the class we've chosen the default constructor)
 * as well as set the fields marked with Inject annotation to instances made by keys from their type and annotation
 * (see why second part of key is an annotation? :D)
 * <p>
 * Datakernel DI also comes with high-level reflection DSL facade to make it similar to other DI frameworks for the ease of transition
 * and also because the reflection DSL is pretty declarative and consise.
 */
public final class ReflectionExample {

	interface MessageSender {
		void send(String message);
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@NameAnnotation
	@interface SecondKey {
	}

	@Inject
	static class ConsoleMessageSenderImpl implements MessageSender {
		@Override
		public void send(String message) {
			System.out.println("received message: " + message);
		}
	}

	static class LoggingMessageSenderImpl implements MessageSender {
		private final Logger logger;

		@Inject
		public LoggingMessageSenderImpl(Logger logger) {
			this.logger = logger;
		}

		@Override
		public void send(String message) {
			logger.log(INFO, () -> "received message: " + message);
		}
	}

	@Inject
	static class Application {
		@Inject
		@Named("first")
		private MessageSender sender;

		@Inject
		@SecondKey
		private MessageSender logger;

		void hello() {
			sender.send("hello from application");
			logger.send("logged greeting");
		}
	}

	static class ApplicationModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(Logger.class).toInstance(Logger.getLogger("example"));

			// it knows how to make instances of impls because they have inject annotations
			// allowing us to know how they can be created
			// we never automagically create instances of unaware classes

			bind(MessageSender.class).annotatedWith(Name.of("first")).to(ConsoleMessageSenderImpl.class);
			bind(Key.of(MessageSender.class, SecondKey.class)).to(LoggingMessageSenderImpl.class);

			// same as above, just trigger the automatic factory generation from the marked constructor
			bind(Application.class);
		}
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new ApplicationModule());
		Application application = injector.getInstance(Application.class);

		application.hello();
	}
}
