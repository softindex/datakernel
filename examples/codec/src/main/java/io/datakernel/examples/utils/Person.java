/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.examples.utils;

import java.time.LocalDate;

public class Person {
	private final int id;
	private final String name;
	private LocalDate dateOfBirth;

	public Person(int id, String name, LocalDate dateOfBirth) {
		this.name = name;
		this.id = id;
		this.dateOfBirth = dateOfBirth;
	}

	public int getId() {
		return id;
	}

	String getName() {
		return name;
	}

	LocalDate getDateOfBirth() {
		return dateOfBirth;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Person person = (Person) o;

		if (id != person.id) return false;
		if (!name.equals(person.name)) return false;
		return dateOfBirth.equals(person.dateOfBirth);
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + name.hashCode();
		result = 31 * result + dateOfBirth.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Person{" +
				"id=" + id +
				", name='" + name + '\'' +
				", dateOfBirth=" + dateOfBirth +
				'}';
	}
}
