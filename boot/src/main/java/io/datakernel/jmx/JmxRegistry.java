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

import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.datakernel.jmx.annotation.JmxMBean;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static io.datakernel.util.Preconditions.check;

public final class JmxRegistry {
	private final MBeanServer mbs;
	private final DynamicMBeanFactory mbeanFactory;

	public JmxRegistry(MBeanServer mbs, DynamicMBeanFactory mbeanFactory) {
		this.mbs = mbs;
		this.mbeanFactory = mbeanFactory;
	}

	public void registerPoolOfInstances(Key<?> key, List<?> instances) {

	}

	public void registerInstance(Key<?> key, Object instance) {
		DynamicMBean mbean;
		try {
			mbean = mbeanFactory.createFor(instance);
		} catch (Exception e) {
			// TODO(vmykhalko): is it correct exception handling policy ?
			throw new RuntimeException(e);
		}
		String domain = key.getTypeLiteral().getRawType().getPackage().getName();
		Annotation annotation = key.getAnnotation();
		String name;
		if (annotation == null) {
			name = domain + ":type=" + key.getTypeLiteral().getRawType().getSimpleName();
		} else {
			Method[] methods = annotation.annotationType().getDeclaredMethods();
			if (methods.length == 0) {
				name = domain + ":type=" + annotation.annotationType().getSimpleName();
			} else if (methods.length == 1 && methods[0].getName() == "value") {
				Object value;
				try {
					value = methods[0].invoke(annotation);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
				if (value == null) {
					String errorMsg = "@" + annotation.annotationType().getName() + "." +
							methods[0].getName() + "() returned null";
					throw new NullPointerException(errorMsg);
				}
				name = domain + ":" + annotation.annotationType().getSimpleName() + "=" + value.toString();
			} else {
				name = domain + ":";
				for (Method method : methods) {
					String nameKey = method.getName();
					String nameValue;
					try {
						Object value = method.invoke(annotation);
						if (value == null) {
							String errorMsg = "@" + annotation.annotationType().getName() + "." +
									method.getName() + "() returned null";
							throw new NullPointerException(errorMsg);
						}
						nameValue = value.toString();
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}
					name += nameKey + "=" + nameValue + ",";
				}

				assert name.substring(name.length() - 1).equals(",");

				name = name.substring(0, name.length() - 1);
			}
		}

		ObjectName objectName = null;
		try {
			objectName = new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		}
		try {
			mbs.registerMBean(mbean, objectName);
		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isJmxMBean(Object instance) {
		return instance.getClass().isAnnotationPresent(JmxMBean.class);
	}

	public void unregister(Key<?> key) {

	}
}
