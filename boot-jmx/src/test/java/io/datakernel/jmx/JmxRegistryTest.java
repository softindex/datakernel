/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.jmx;

import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.jmx.api.ConcurrentJmxMBean;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static io.datakernel.jmx.helper.CustomMatchers.objectname;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public class JmxRegistryTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private DynamicMBeanFactory mbeanFactory = context.mock(DynamicMBeanFactory.class);
	private DynamicMBean dynamicMBean = context.mock(DynamicMBean.class);
	private JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();
	private final MBeanSettings settings = MBeanSettings.defaultSettings();

	@Test
	public void registerSingletonInstanceWithout_Annotation_AndComposeAppropriateObjectName() throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(service)), with(any(MBeanSettings.class)), with(true));
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key_1 = Key.of(ServiceStub.class);
		jmxRegistry.registerSingleton(key_1, service, settings);
	}

	@Test
	public void registerSingletonInstanceOfAnonymousClass() throws Exception {
		ConcurrentJmxMBean anonymousMBean = new ConcurrentJmxMBean(){};
		Class<? extends ConcurrentJmxMBean> anonymousMBeanClass = anonymousMBean.getClass();
		String packageName = anonymousMBeanClass.getPackage().getName();
		String typeName = anonymousMBeanClass.getName().substring(packageName.length() + 1);
		assertThat(typeName, startsWith("JmxRegistryTest$"));

		context.checking(new Expectations() {{
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(anonymousMBean)), with(any(MBeanSettings.class)), with(true));
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean),
					with(objectname(packageName + ":type=" + typeName)));
		}});

		Key<?> key_1 = Key.of(anonymousMBeanClass);
		jmxRegistry.registerSingleton(key_1, anonymousMBean, settings);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithoutParameters_AndComposeAppropriateObjectName()
			throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(service)), with(any(MBeanSettings.class)), with(true));
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerSingleton(key, service, settings);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithOneDefaultParameter_AndComposeAppropriateObjectName()
			throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(service)), with(any(MBeanSettings.class)), with(true));
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(with(dynamicMBean), with(objectname(domain + ":type=ServiceStub,Group=major")));
		}});

		Group groupAnnotation = createGroupAnnotation("major");
		Key<?> key = Key.of(ServiceStub.class, groupAnnotation);
		jmxRegistry.registerSingleton(key, service, settings);
	}

	@Test
	public void registerSingletonInstanceWith_AnnotationWithSeveralParameters_AndComposeAppropriateObjectName()
			throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(service)), with(any(MBeanSettings.class)), with(true));
			will(returnValue(dynamicMBean));

			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean), with(objectname(domain + ":type=ServiceStub,threadId=1,threadName=thread-one")));
		}});

		ComplexAnnotation complexAnnotation = createComplexAnnotation(1, "thread-one");
		Key<?> key = Key.of(ServiceStub.class, complexAnnotation);
		jmxRegistry.registerSingleton(key, service, settings);

	}

	@Test
	public void unregisterSingletonInstance() throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).unregisterMBean(with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterSingleton(key, service);
	}

	@Test
	public void registerWorkers_andComposeAppropriateObjectNames() throws Exception {
		WorkerPool workerPool = getWorkerPool();
		WorkerPool.Instances<ServiceStub> instances = workerPool.getInstances(ServiceStub.class);

		ServiceStub worker_1 = instances.get(0);
		ServiceStub worker_2 = instances.get(1);
		ServiceStub worker_3 = instances.get(2);

		context.checking(new Expectations() {{
			// creating DynamicMBeans for each worker separately
			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(worker_1)), with(any(MBeanSettings.class)), with(false));
			will(returnValue(dynamicMBean));

			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(worker_2)), with(any(MBeanSettings.class)), with(false));
			will(returnValue(dynamicMBean));

			allowing(mbeanFactory)
					.createDynamicMBean(with(singletonList(worker_3)), with(any(MBeanSettings.class)), with(false));
			will(returnValue(dynamicMBean));

			// creating DynamicMBean for worker pool
			allowing(mbeanFactory)
					.createDynamicMBean(with(asList(worker_1, worker_2, worker_3)),
							with(any(MBeanSettings.class)), with(true));
			will(doAll());
			will(returnValue(dynamicMBean));

			// checking calls and names for each worker separately
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-0")));
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-1")));
			oneOf(mBeanServer).registerMBean(
					with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-2")));

			// checking calls and names for worker_pool DynamicMBean
			oneOf(mBeanServer).registerMBean(with(dynamicMBean),
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker")));

		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerWorkers(workerPool, key, asList(worker_1, worker_2, worker_3), settings);
	}

	@Test
	public void unregisterWorkers() throws Exception {
		WorkerPool workerPool = getWorkerPool();
		WorkerPool.Instances<ServiceStub> instances = workerPool.getInstances(ServiceStub.class);

		ServiceStub worker_1 = instances.get(0);
		ServiceStub worker_2 = instances.get(1);
		ServiceStub worker_3 = instances.get(2);

		context.checking(new Expectations() {{
			// checking calls and names for each worker separately
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-0")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-1")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-2")));

			// checking calls and names for worker_pool DynamicMBean
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker")));

		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterWorkers(workerPool, key, asList(worker_1, worker_2, worker_3));
	}

	private WorkerPool getWorkerPool() {
		Injector injector = Injector.of(WorkerPoolModule.create(), new AbstractModule() {
			@Override
			protected void configure() {
				bind(ServiceStub.class).in(Worker.class).to(ServiceStub::new);
			}
		});
		WorkerPools pools = injector.getInstance(WorkerPools.class);
		return pools.createPool(3);
	}

	// annotations
	@Retention(RetentionPolicy.RUNTIME)
	@NameAnnotation
	public @interface BasicService {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@NameAnnotation
	public @interface Group {
		String value() default "";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@NameAnnotation
	public @interface ComplexAnnotation {
		int threadId() default -1;

		String threadName() default "defaultThread";
	}

	// helpers for creating annotations
	public static BasicService createBasicServiceAnnotation() {
		return new BasicService() {
			@Override
			public Class<? extends Annotation> annotationType() {
				return BasicService.class;
			}
		};
	}

	public static Group createGroupAnnotation(String value) {
		return new Group() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return Group.class;
			}

			@Override
			public String value() {
				return value;
			}
		};
	}

	public static ComplexAnnotation createComplexAnnotation(int threadId, String threadName) {
		return new ComplexAnnotation() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return ComplexAnnotation.class;
			}

			@Override
			public int threadId() {
				return threadId;
			}

			@Override
			public String threadName() {
				return threadName;
			}
		};
	}

	// helper classes
	public static final class ServiceStub implements ConcurrentJmxMBean {

	}
}
