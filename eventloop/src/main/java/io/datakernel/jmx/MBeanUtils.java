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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.util.Collection;

public final class MBeanUtils {
	private static final Logger logger = LoggerFactory.getLogger(MBeanUtils.class);

	public static void register(MBeanServer server, ObjectName objectName, Object mbean) {
		if (server == null)
			return;
		logger.debug("Registering MBean {}: {}", objectName, mbean);
		try {
			server.registerMBean(mbean, objectName);
		} catch (InstanceAlreadyExistsException e) {
			logger.error("MBean {} already registered: {}", objectName, e.toString());
			throw new RuntimeException(e);
		} catch (MBeanRegistrationException e) {
			logger.error("Registration failed for MBean {}: {}", objectName, e.toString());
			throw new RuntimeException(e);
		} catch (NotCompliantMBeanException e) {
			logger.warn("Not compliant MBean {}", objectName);
		}
	}

	public static void register(MBeanServer server, String domain, String type, Collection<?> mbeans) {
		if (server == null)
			return;
		for (Object mbean : mbeans) {
			register(server, MBeanFormat.name(domain, type, mbean.getClass()), mbean);
		}
	}

	public static void unregisterIfExists(MBeanServer server, ObjectName objectName) {
		try {
			server.unregisterMBean(objectName);
		} catch (MBeanRegistrationException e) {
			logger.error("Could not unregister MBean {}: {}", objectName, e.toString());
		} catch (InstanceNotFoundException e) {
			logger.trace("MBean does not exists {}", objectName);
		}
	}
}
