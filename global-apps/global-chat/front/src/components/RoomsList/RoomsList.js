import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class RoomsList extends React.Component {

  onRemoveContact(room) {
    const publicKey = room.participants.find(publicKey => publicKey !== this.props.publicKey);
    const name = this.props.contacts.get(publicKey).name;
    return this.props.onRemoveContact(publicKey, name);
  }

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
                  isContactsTab={this.props.isContactsTab}
                  contacts={this.props.contacts}
                  publicKey={this.props.publicKey}
                  onAddContact={this.props.onAddContact}
                  onRemoveContact={this.onRemoveContact.bind(this, room)}
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
