package io.datakernel.examples;

public interface LoginService {

	boolean authorize(String login, String password);

	void register(String login, String password);
}
