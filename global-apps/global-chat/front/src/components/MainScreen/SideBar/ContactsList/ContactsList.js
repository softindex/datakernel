import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import Contact from "./ContactItem/ContactItem";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
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

  sortContacts = (contactsList) => {
    return [...contactsList].sort((a, b) => a[1].name.localeCompare(b[1].name));
  };

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
              {this.sortContacts([...contacts]).map(([pubKey, {name}]) => {
                let roomId = '';

                for (const [roomPublicKey, {participants}] of rooms) {
                  if (participants.includes(pubKey) && participants.length === 2) {
                    roomId = roomPublicKey;
                  }
                }

                return (
                    <Link to={this.getRoomPath(roomId)} className={classes.link}>
                      <Contact
                        name={name}
                        showDeleteButton={true}
                        dialogFormContext={true}
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

export default withStyles(contactsListStyles)(ContactsList);
