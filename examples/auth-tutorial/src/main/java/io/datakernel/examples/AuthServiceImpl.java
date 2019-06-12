package io.datakernel.examples;

import java.util.HashMap;
import java.util.Map;
//[START EXAMPLE]
public class AuthServiceImpl implements AuthService {
	private Map<String, String> credentials = new HashMap<>();

	@Override
	public boolean authorize(String login, String password) {
		String foundPassword = credentials.get(login);
		return foundPassword != null && foundPassword.equals(password);
	}

	@Override
	public void register(String login, String password) {
		credentials.put(login, password);
	}
}
//[END EXAMPLE]
