package decoder;

import java.util.List;

interface ContactDAO {
	List<Contact> getAll();
	Contact get(int id);
	void add(Contact user);
}
