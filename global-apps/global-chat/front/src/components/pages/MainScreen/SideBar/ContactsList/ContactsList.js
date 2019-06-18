import React from "react";
import connectService from "../../../../../common/connectService";
import {withStyles} from '@material-ui/core';
import Contact from "../../../../ContactItem/Contact";
import ContactsContext from "../../../../../modules/contacts/ContactsContext";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";
import RoomsContext from "../../../../../modules/rooms/RoomsContext";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class ContactsList extends React.Component {
  componentDidMount() {
    this.props.contactsService.init();
  }

  componentWillUnmount() {
    this.props.contactsService.stop();
  }

  onRemoveContact(pubKey, name) {
    return this.props.removeContact(pubKey, name);
  }

  onChatCreate(name, pubKey) {
    return this.props.createDialog(name, [pubKey]);
  }

  render() {
    const {classes, ready, contacts} = this.props;
    return (
      <>
        {!ready && (
          <Grow in={!ready}>
            <div className={classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {ready && (
          <div className={classes.contactList}>
            <List>
              {[...contacts].map(([pubKey, {name}]) =>
                <Contact
                  name={name}
                  showDeleteButton={true}
                  onRemoveContact={this.onRemoveContact.bind(this, pubKey, name)}
                  onChatCreate={this.onChatCreate.bind(this, name, pubKey)}
                />
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default connectService(
  ContactsContext,
  ({ready, contacts}, contactsService) => ({
    contactsService, ready, contacts,
    removeContact(pubKey, name) {
      return contactsService.removeContact(pubKey, name);
    }
  })
)(
  connectService(
    RoomsContext,
    ({ready, rooms}, roomsService) => ({
      rooms, roomsService,
      createDialog(name, participants) {
        let roomExists = false;
       roomsService.state.rooms.map(room => {
          if (room.name === name) {
            roomExists = true;
          }
       });
       if (!roomExists) {
         return roomsService.createDialog(name, participants);
       }
      }
    })
  )(
    withStyles(contactsListStyles)(ContactsList)
  )
);
