package io.datakernel;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 24.07.19.
 * @since 3.0.0
 */
@State(Scope.Benchmark)
public class DkDiScopesBenchmark {

    static class Kitchen {
        private final int places;

        @Inject
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

        @Inject
        public Sugar() {
            this.name = "Sugarella";
            this.weight = 10.f;
        }
        //[END REGION_8]

        public Sugar(String name, float weight) {
            this.name = name;
            this.weight = weight;
        }

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

        @Inject
        public Butter() {
            this.weight = 10.f;
            this.name = "Butter";
        }

        public Butter(String name, float weight) {
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

    static class Flour {
        private float weight;
        private String name;

        @Inject
        public Flour() { }

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

        @Inject
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

        @Inject
        Cookie1(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie2 {
        private final Pastry pastry;

        @Inject
        Cookie2(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie3 {
        private final Pastry pastry;

        @Inject
        Cookie3(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie4 {
        private final Pastry pastry;

        @Inject
        Cookie4(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie5 {
        private final Pastry pastry;

        @Inject
        Cookie5(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie6 {
        private final Pastry pastry;

        @Inject
        Cookie6(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class Cookie7 {
        private final Pastry pastry;

        @Inject
        Cookie7(Pastry pastry) {
            this.pastry = pastry;
        }

        public Pastry getPastry() {
            return pastry;
        }
    }

    static class TORT {
        private final Cookie1 c1;
        private final Cookie2 c2;
        private final Cookie3 c3;
        private final Cookie4 c4;
        private final Cookie5 c5;
        private final Cookie6 c6;
        private final Cookie7 c7;

        public Cookie4 getC4() {
            return c4;
        }

        @Inject
        public TORT(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6, Cookie7 c7) {
            this.c1 = c1;
            this.c2 = c2;
            this.c3 = c3;
            this.c4 = c4;
            this.c5 = c5;
            this.c6 = c6;
            this.c7 = c7;
        }
    }

    AbstractModule cookbook;
    Injector injector;

    public static final io.datakernel.di.core.Scope ORDER_SCOPE = io.datakernel.di.core.Scope.of(Order.class);

    @Setup
    public void setup() {
        cookbook = new AbstractModule() {
            @Override
            protected void configure() {
                super.configure();
                final SimpleScope orderScope = new SimpleScope();
            }

            @Provides
            Kitchen kitchen() { return new Kitchen(); }

            @Provides
            @Order
            Sugar sugar() { return new Sugar("Sugarello", 10.f); }

            @Provides
            @Order
            Butter butter() { return new Butter("Kyivmlyn", 20.0f); }

            @Provides
            @Order
            Flour flour() { return new Flour("Kyivska", 100.0f); }

            @Provides
            @Order
            Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
                return new Pastry(sugar, butter, flour);
            }

            @Provides
            @Order
            Cookie1 cookie1(Pastry pastry) {
                return new Cookie1(pastry);
            }

            @Provides
            @Order
            Cookie2 cookie2(Pastry pastry) {
                return new Cookie2(pastry);
            }

            @Provides
            @Order
            Cookie3 cookie3(Pastry pastry) {
                return new Cookie3(pastry);
            }

            @Provides
            @Order
            Cookie4 cookie4(Pastry pastry) {
                return new Cookie4(pastry);
            }

            @Provides
            @Order
            Cookie5 cookie5(Pastry pastry) {
                return new Cookie5(pastry);
            }

            @Provides
            @Order
            Cookie6 cookie6(Pastry pastry) {
                return new Cookie6(pastry);
            }

            @Provides
            @Order
            Cookie7 cookie7(Pastry pastry) {
                return new Cookie7(pastry);
            }

            @Provides
            @Order
            TORT tort(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6, Cookie7 c7) {
                return new TORT(c1, c2, c3, c4, c5, c6, c7);
            }

        };

        injector = Injector.of(cookbook);

    }

    Cookie1 cookie1;
    Cookie2 cookie2;
    Cookie3 cookie3;
    Cookie4 cookie4;
    Cookie5 cookie5;
    Cookie6 cookie6;
    Cookie7 cookie7;

    @Param({"0", "1", "10"})
    int arg;

    @Benchmark
    @OutputTimeUnit(value = TimeUnit.NANOSECONDS)
    public void testMethod(Blackhole blackhole) {
        Kitchen kitchen = injector.getInstance(Kitchen.class);
        for (int i = 0; i < arg; ++i) {
            Injector subinjector = injector.enterScope(ORDER_SCOPE);
            cookie1 = subinjector.getInstance(Cookie1.class);
            cookie2 = subinjector.getInstance(Cookie2.class);
            cookie3 = subinjector.getInstance(Cookie3.class);
            cookie4 = subinjector.getInstance(Cookie4.class);
            cookie5 = subinjector.getInstance(Cookie5.class);
            cookie6 = subinjector.getInstance(Cookie6.class);
            cookie7 = subinjector.getInstance(Cookie7.class);
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
				.include(DkDiScopesBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}

//  master (24.07)
//	Benchmark                       (arg)  Mode  Cnt     Score     Error  Units
//	DkDiScopesBenchmark.testMethod      0  avgt   20    50.301 ±   0.340  ns/op
//	DkDiScopesBenchmark.testMethod      1  avgt   20   934.922 ±  16.528  ns/op
//	DkDiScopesBenchmark.testMethod     10  avgt   20  8658.261 ± 138.339  ns/op

// 3.0.1 threadsafe = true
// 	Benchmark                       (arg)  Mode  Cnt     Score    Error  Units
//	DkDiScopesBenchmark.testMethod      0  avgt   20    41.164 ±  0.232  ns/op
//	DkDiScopesBenchmark.testMethod      1  avgt   20   721.699 ±  8.486  ns/op
//	DkDiScopesBenchmark.testMethod     10  avgt   20  7108.043 ± 49.030  ns/op

// 3.0.1 threadsafe = false
//	Benchmark                       (arg)  Mode  Cnt     Score    Error  Units
//	DkDiScopesBenchmark.testMethod      0  avgt   20    40.871 ±  0.138  ns/op
//	DkDiScopesBenchmark.testMethod      1  avgt   20   649.634 ±  5.981  ns/op
//	DkDiScopesBenchmark.testMethod     10  avgt   20  6379.876 ± 87.901  ns/op

//  Guice scoped
// 	Benchmark                          (arg)  Mode  Cnt      Score      Error  Units
//	GuiceDiScopesBenchmark.testMethod      0  avgt   20    105.303 ±   28.944  ns/op
//	GuiceDiScopesBenchmark.testMethod      1  avgt   20   2424.716 ±  207.163  ns/op
//	GuiceDiScopesBenchmark.testMethod     10  avgt   20  25431.301 ± 2312.918  ns/op
//
// Guice unscoped (broken semantic)
//	Benchmark                          (arg)  Mode  Cnt      Score     Error  Units
//	GuiceDiScopesBenchmark.testMethod      0  avgt   20     82.886 ±   3.599  ns/op
//	GuiceDiScopesBenchmark.testMethod      1  avgt   20   2071.644 ± 149.757  ns/op
//	GuiceDiScopesBenchmark.testMethod     10  avgt   20  18504.430 ± 752.133  ns/op



