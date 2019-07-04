import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import RoomItem from "./RoomItem/RoomItem";
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


  onClickLink = (roomId) => {
    const {contactId} = this.getRoomPath(roomId);
    if (this.props.rooms.get(contactId)) {
      const room = this.props.rooms.get(contactId);
      if (room.virtual) {
        this.onChatCreate(contactId);
      }
    }
  };

  render() {
    const {classes, ready, rooms, roomsService} = this.props;
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
          <div className={classes.roomsList}>
            <List>
              {[...rooms].map(([roomId, room]) =>
                (
                  <RoomItem
                    roomId={roomId}
                    room={room}
                    onClickLink={this.onClickLink}
                    getRoomPath={this.getRoomPath}
                    quitRoom={this.quitRoom.bind(this, roomId)}
                    roomsService={roomsService}
                    showMenuIcon={true}
                    contacts={this.props.contacts}
                    publicKey={this.props.publicKey}
                    addContact={this.props.addContact}
                  />
                )
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default withStyles(roomsListStyles)(RoomsList);
