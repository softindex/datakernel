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

package io.datakernel.codegen;


import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.objectweb.asm.commons.Method;

/**
 * Represents the scope of a new class.
 * Inspired by reflectasm.
 * 
 * @author Nico Hezel
 */
public class ClassScope<T> {

	private final Class<T> mainType;
	private final Set<Class<?>> parentClasses = new LinkedHashSet<>();
	
	private final Map<String, Class<?>> fields = new LinkedHashMap<>();
	private final Map<String, Class<?>> staticFields = new LinkedHashMap<>();
	private final List<Method> constructors = new ArrayList<>();
	private final Set<Method> methods = new LinkedHashSet<>();
	private final Set<Method> staticMethods = new LinkedHashSet<>();
	

	/**
	 * Collect all non-private methods and fields of the given classes.
	 * 
	 * @param mainType might be a super class
	 * @param types might be interfaces 
	 */
	public ClassScope(Class<T> mainType, List<Class<?>> types) {
		this.mainType = mainType;
		
	    collectMembers(mainType);
	    for (Class<?> type : types)
	    	if(type.isInterface())
	    		collectMembers(type);
	}

	 private void addNonPrivateField(java.lang.reflect.Field[] arr) {
	        for(java.lang.reflect.Field e : arr) {
	            if(Modifier.isPrivate(e.getModifiers())) continue;
	            
	            if(Modifier.isStatic(e.getModifiers()))
	            	staticFields.put(e.getName(), e.getType());
	            else
	            	fields.put(e.getName(), e.getType());
	        }
	    }
	 
    private void addNonPrivateMethod(java.lang.reflect.Method[] arr) {
        for(java.lang.reflect.Method e : arr) {
            if(Modifier.isPrivate(e.getModifiers())) continue;
            
            if(Modifier.isStatic(e.getModifiers()))
            	staticMethods.add(Method.getMethod(e));
            else
            	methods.add(Method.getMethod(e));
        }
    }

    private void recursiveAddInterfaceMethodsToList(Class<?> interfaceType) {
    	addNonPrivateMethod(interfaceType.getDeclaredMethods());
        for (Class<?> nextInterface : interfaceType.getInterfaces()) {
            recursiveAddInterfaceMethodsToList(nextInterface);
        }
    }

    private void collectMembers(Class<?> type) {
    	parentClasses.add(type);
        if (type.isInterface()) {
            recursiveAddInterfaceMethodsToList(type);
            return;
        }
        boolean search = true;
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            if (Modifier.isPrivate(constructor.getModifiers())) continue;
            int length = constructor.getParameterTypes().length;
            if (search) {
                switch (length) {
                    case 0:
                        constructors.add(0, Method.getMethod(constructor));
                        search = false;
                        break;
                    case 1:
                        constructors.add(0, Method.getMethod(constructor));
                        break;
                    default:
                        constructors.add(Method.getMethod(constructor));
                        break;
                }
            }
        }
        Class<?> nextClass = type;
        while (nextClass != Object.class) {
            addNonPrivateField(nextClass.getDeclaredFields());
            addNonPrivateMethod(nextClass.getDeclaredMethods());
            for (Class<?> nextInterface : nextClass.getInterfaces())
            	recursiveAddInterfaceMethodsToList(nextInterface);
            nextClass = nextClass.getSuperclass();
        }
    }
	

	public void addField(String field, Class<?> fieldClass) {
		fields.put(field, fieldClass);
	}

	public void addStaticField(String field, Class<?> fieldClass) {
		staticFields.put(field, fieldClass);
	}

	public void addMethod(Method method) {
		methods.add(method);
	}

	public void addStaticMethod(Method method) {
		staticMethods.add(method);
	}
	
	public Class<T> getMainType() {
		return mainType;
	}
	
	public Set<Class<?>> getParentClasses() {
		return parentClasses;
	}
	
	public List<Method> getConstructors() {
		return constructors;
	}
	
	public Map<String, Class<?>> getFields() {
		return fields;
	}
	
	public Map<String, Class<?>> getStaticFields() {
		return staticFields;
	}
	
	public Set<Method> getMethods() {
		return methods;
	}
	
	public Set<Method> getStaticMethods() {
		return staticMethods;
	}
}