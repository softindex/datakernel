import React from "react";

class AddContactForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {pubKey: '', name: '', error: null};

    this.handlePKChange = this.handlePKChange.bind(this);
    this.handleNameChange = this.handleNameChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handlePKChange(event) {
    this.setState({pubKey: event.target.value});
  }

  handleNameChange(event) {
    this.setState({name: event.target.value});
  }

  handleSubmit(event) {
    event.preventDefault();
    this.props.contactsService.addContact(this.state.pubKey, this.state.name)
      .catch(e => {
        console.log(e);
        return this.setState({error: e});
      })
  }

  render() {
    return <form onSubmit={this.handleSubmit}>
      <label>
        Key:
        <input type="text" value={this.state.pubKey} onChange={this.handlePKChange}/>
      </label>
      <label>
        Name:
        <input type="text" value={this.state.contact} onChange={this.handleNameChange}/>
      </label>
      <input type="submit" value="Add contact"/>
    </form>;
  }
}

export default AddContactForm;
