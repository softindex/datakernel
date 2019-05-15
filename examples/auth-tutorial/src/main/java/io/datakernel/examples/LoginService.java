package io.datakernel.examples;

//[START EXAMPLE]
public interface LoginService {

	boolean authorize(String login, String password);

	void register(String login, String password);
}
//[END EXAMPLE]
