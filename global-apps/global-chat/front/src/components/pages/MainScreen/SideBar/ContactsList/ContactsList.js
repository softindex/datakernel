import React from "react";
import path from "path";
import connectService from "../../../../../common/connectService";
import {withStyles} from '@material-ui/core';
import Contact from "./ContactItem/ContactItem";
import ContactsContext from "../../../../../modules/contacts/ContactsContext";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";
import RoomsContext from "../../../../../modules/rooms/RoomsContext";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
import AccountContext from "../../../../../modules/account/AccountContext";
import {Link} from "react-router-dom";

class ContactsList extends React.Component {
  componentDidMount() {
    this.props.contactsService.init();
  }

  componentWillUnmount() {
    this.props.contactsService.stop();
  }

  onRemoveContact(pubKey, name, event) {
    event.preventDefault();
    event.stopPropagation();
    return this.props.removeContact(pubKey, name);
  }

  onChatCreate(participantId) {
    return this.props.createDialog(participantId);
  }

  getRoomPath = (roomId) => {
    return path.join('/room', roomId || '');
  };

  render() {
    const {classes, ready, contacts, rooms} = this.props;
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
              {[...contacts].map(([pubKey, {name}]) => {
                  let roomId = '';
                  return ([...rooms].map(([roomPublicKey, {participants}]) => {
                    if (participants.includes(pubKey) && participants.length === 2) {
                      roomId = roomPublicKey;
                    }
                  })) && (
                    <Link to={this.getRoomPath(roomId)} className={classes.link}>
                      <Contact
                        name={name}
                        showDeleteButton={true}
                        onRemoveContact={this.onRemoveContact.bind(this, pubKey, name)}
                        onChatCreate={this.onChatCreate.bind(this, pubKey)}
                        roomId={roomId}
                      />
                    </Link>
                )}
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
      createDialog(participantId) {
        return roomsService.createDialog(participantId);
      }
    })
  )(
    withStyles(contactsListStyles)(ContactsList)
  )
);
