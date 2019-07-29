package io.datakernel;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @Author is Alex Syrotenko (@pantokrator)
 * Created on 26.07.19.
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Configuration
public class SpringDiBenchmark {


	static class Kitchen {
		private final int places;

		Kitchen() {
			this.places = 1;
		}

		public int getPlaces() {
			return places;
		}
	}


	static class Sugar {
		private final String name;
		private final float weight;

		@Autowired
		public Sugar() {
			this.name = "Sugarella";
			this.weight = 10.f;
		}
		//[END REGION_8]

		public String getName() {
			return name;
		}

		public float getWeight() {
			return weight;
		}
	}


	static class Butter {
		private float weight;
		private String name;

		@Autowired
		public Butter() {
			this.weight = 10.f;
			this.name = "Butter";
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}


	static class Flour {
		private float weight;
		private String name;

		@Autowired
		public Flour() {
			this.name = "Kyivska";
			this.weight =  100.0f;
		}

		public Flour(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}


	static class Pastry {
		private final Sugar sugar;
		private final Butter butter;
		private final Flour flour;

		@Autowired
		Pastry(Sugar sugar, Butter butter, Flour flour) {
			this.sugar = sugar;
			this.butter = butter;
			this.flour = flour;
		}

		public Flour getFlour() {
			return flour;
		}

		public Sugar getSugar() {
			return sugar;
		}

		public Butter getButter() {
			return butter;
		}
	}


	static class Cookie1 {
		private final Pastry pastry;

		@Autowired
		Cookie1(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie2 {
		private final Pastry pastry;

		@Autowired
		Cookie2(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie3 {
		private final Pastry pastry;

		@Autowired
		Cookie3(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie4 {
		private final Pastry pastry;

		@Autowired
		Cookie4(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie5 {
		private final Pastry pastry;

		@Autowired
		Cookie5(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie6 {
		private final Pastry pastry;

		@Autowired
		Cookie6(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	static class Cookie7 {
		private final Pastry pastry;

		@Autowired
		Cookie7(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}


	@Bean
	Kitchen kitchen() { return new Kitchen(); }

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Sugar sugar() { return new Sugar(); }

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Butter butter() { return new Butter(); }

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Flour flour() { return new Flour(); }

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
		return new Pastry(sugar, butter, flour);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie1 cookie1(Pastry pastry) {
		return new Cookie1(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie2 cookie2(Pastry pastry) {
		return new Cookie2(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie3 cookie3(Pastry pastry) {
		return new Cookie3(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie4 cookie4(Pastry pastry) {
		return new Cookie4(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie5 cookie5(Pastry pastry) {
		return new Cookie5(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie6 cookie6(Pastry pastry) {
		return new Cookie6(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie7 cookie7(Pastry pastry) {
		return new Cookie7(pastry);
	}



	ConfigurableApplicationContext context;

	Cookie1 cookie1;
	Cookie2 cookie2;
	Cookie3 cookie3;
	Cookie4 cookie4;
	Cookie5 cookie5;
	Cookie6 cookie6;
	Cookie7 cookie7;

	@Param({"0", "1", "10"})
	int arg;

	@Setup
	public void setup() {
		context = new AnnotationConfigApplicationContext(SpringDiBenchmark.class);
	}

	@Benchmark
	public void measure(Blackhole blackhole) {
		Kitchen kitchen = context.getBean(Kitchen.class);
		Cookie1 cookie = context.getBean(Cookie1.class);
		for (int i = 0; i < arg; ++i) {
			cookie1 = context.getBean(Cookie1.class);
			cookie2 = context.getBean(Cookie2.class);
			cookie3 = context.getBean(Cookie3.class);
			cookie4 = context.getBean(Cookie4.class);
			cookie5 = context.getBean(Cookie5.class);
			cookie6 = context.getBean(Cookie6.class);
			cookie7 = context.getBean(Cookie7.class);
			assert cookie.equals(cookie1);
			blackhole.consume(cookie1);
			blackhole.consume(cookie2);
			blackhole.consume(cookie3);
			blackhole.consume(cookie4);
			blackhole.consume(cookie5);
			blackhole.consume(cookie6);
			blackhole.consume(cookie7);
		}
		blackhole.consume(kitchen);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SpringDiBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.MICROSECONDS)
				.build();

		new Runner(opt).run();
	}
}

// Benchmark                  (arg)  Mode  Cnt     Score   Error  Units
//SpringDiBenchmark.measure      0  avgt   20    16.498 ± 0.189  us/op
//SpringDiBenchmark.measure      1  avgt   20   133.197 ± 1.291  us/op
//SpringDiBenchmark.measure     10  avgt   20  1180.808 ± 8.212  us/op
//
//	Benchmark                  (arg)  Mode  Cnt     Score    Error  Units
//	SpringDiBenchmark.measure      0  avgt   20    17.056 ±  0.244  us/op
//	SpringDiBenchmark.measure      1  avgt   20   137.238 ±  1.658  us/op
//	SpringDiBenchmark.measure     10  avgt   20  1255.057 ± 46.047  us/op
