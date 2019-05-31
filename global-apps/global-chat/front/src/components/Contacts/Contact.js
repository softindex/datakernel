import React from "react";

class Contact extends React.Component {
  constructor(props) {
    super(props);
    this.removeContact = this.removeContact.bind(this);
    this.changeName = this.changeName.bind(this);
  }

  removeContact() {
    this.props.contactsService.removeContact(this.props.pubKey, this.props.name);
  }

  changeName() {
    // this.props.contactsService.addContact(this.props.pubKey, this.state.newName);
  }

  render() {
    return <li>
      {this.props.name}
      <button onClick={this.removeContact}>
        Remove
      </button>
    </li>;
  }
}

export default Contact;
