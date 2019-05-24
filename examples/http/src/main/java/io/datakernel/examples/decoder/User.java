package io.datakernel.examples.decoder;

final class User {
	private final String name;
	private final Integer age;
	private final Role role;

	public User(String name, Integer age, Role role) {
		this.name = name;
		this.age = age;
		this.role = role;
	}

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	@Override
	public String toString() {
		return "User{" +
				"name='" + name + '\'' +
				", age=" + age +
				", role=" + role +
				'}';
	}
}
