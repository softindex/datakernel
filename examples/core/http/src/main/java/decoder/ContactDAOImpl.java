package decoder;

import java.util.ArrayList;
import java.util.List;

public final class ContactDAOImpl implements ContactDAO {
	private List<Contact> userList = new ArrayList<>();

	@Override
	public List<Contact> getAll() {
		return userList;
	}

	@Override
	public Contact get(int id) {
		return userList.get(id);
	}

	@Override
	public void add(Contact user) {
		userList.add(user);
	}
}
