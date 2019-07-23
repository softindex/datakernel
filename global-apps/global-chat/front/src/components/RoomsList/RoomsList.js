import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class RoomsList extends React.Component {
  onChatCreate(participantId) {
    return this.props.createDialog(participantId);
  }

  quitRoom(roomId){
    this.props.quitRoom(roomId);
  }

  getRoomPath = (roomId) => {
    return path.join('/room', roomId || '');
  };

  onRemoveContact(pubKey, name, event) {
    event.preventDefault();
    event.stopPropagation();
    return this.props.removeContact(pubKey, name);
  }

  onClickLink = (roomId) => {
    const {contactId} = this.getRoomPath(roomId);
    if (this.props.rooms.get(contactId)) {
      const room = this.props.rooms.get(contactId);
      if (room.virtual) {
        this.onChatCreate(contactId);
      }
    }
  };

  onContactDelete(room) {
    if (room.participants.length === 2) {
      const participantKey = room.participants.filter(publicKey => publicKey !== this.props.publicKey)[0];
      return this.onRemoveContact.bind(this, participantKey, this.props.contacts.get(participantKey).name)
    }
  };

  render() {
    return (
      <>
        {!this.props.ready && (
          <Grow in={!this.props.ready}>
            <div className={this.props.classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {this.props.ready && (
            <List>
              {[...this.props.rooms].map(([roomId, room]) =>
                (
                  <RoomItem
                    roomId={roomId}
                    room={room}
                    onClickLink={this.onClickLink}
                    getRoomPath={this.getRoomPath}
                    quitRoom={this.quitRoom.bind(this, roomId)}
                    roomsService={this.props.roomsService}
                    showDeleteButton={this.props.showDeleteButton}
                    contacts={this.props.contacts}
                    publicKey={this.props.publicKey}
                    addContact={this.props.addContact}
                    onRemoveContact={this.onContactDelete.bind(this, room)}
                  />
                )
              )}
            </List>
        )}
      </>
    );
  }
}

export default withStyles(roomsListStyles)(RoomsList);
