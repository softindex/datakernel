import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class RoomsList extends React.Component {

  onRemoveContact(pubKey, name) {
    //event.preventDefault();
    //event.stopPropagation();
    return this.props.removeContact(pubKey, name);
  }

  onContactDelete(room) {
    if (room.participants.length === 2) {
      const participantKey = room.participants.find(publicKey => publicKey !== this.props.publicKey);
      return this.onRemoveContact(participantKey, this.props.contacts.get(participantKey).name)
    }
  };

  render() {
    return (
      <>
        {!this.props.roomsReady && (
          <Grow in={!this.props.ready}>
            <div className={this.props.classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {this.props.roomsReady && (
          <List>
            {[...this.props.rooms].map(([roomId, room]) =>
              (
                <RoomItem
                  roomId={roomId}
                  room={room}
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
