import React from "react";
import connectService from "../../common/connectService";
import AddContactForm from "./AddContactForm";
import Contact from "./Contact";
import ContactsContext from "../../modules/contacts/ContactsContext";

class ContactList extends React.Component {
  componentDidMount() {
    this.props.contactsService.init();
  }

  render() {
    let contactsService = this.props.contactsService;
    return <div>Contact List:
      {!contactsService.state.ready ?
        <p>Loading...</p> :
        <div>
          <ul>
            {Array.from(contactsService.state.contacts).map(([pubKey, name]) => <Contact pubKey={pubKey} name={name} contactsService={contactsService}/>)}
          </ul>
          <AddContactForm contactsService={contactsService}/>
        </div>
      }
    </div>;
  }
}

export default connectService(ContactsContext, (state, contactsService) => ({contactsService}))(ContactList);
