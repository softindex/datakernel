import React from "react";
import connectService from "../../../../../common/connectService";
import {withStyles} from '@material-ui/core';
import Contact from "../../../../Contacts/Contact";
import ContactsContext from "../../../../../modules/contacts/ContactsContext";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";

class ContactsList extends React.Component {
  componentDidMount() {
    this.props.contactsService.init();
  }

  componentWillUnmount() {
    this.props.contactsService.stop();
  }

  render() {
    const {classes, ready, contactsService, contacts} = this.props;
    return (
      <>
        {!ready ?
          <p>Loading...</p> :
          <div className={classes.contactList}>
            <List>
              {[...contacts].map(([pubKey, {name}]) =>
                <Contact
                    pubKey={pubKey}
                    name={name}
                    badgeInvisible={this.props.badgeInvisible}
                    contactsService={contactsService}
                  />
              )}
            </List>
          </div>
        }
      </>
    );
  }
}

export default connectService(
  ContactsContext, ({ready, contacts}, contactsService) => ({contactsService, ready, contacts})
)(
  withStyles(contactsListStyles)(ContactsList)
);
