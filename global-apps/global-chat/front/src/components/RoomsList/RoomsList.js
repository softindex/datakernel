import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
import {withRouter} from "react-router-dom";

function RoomsList(props) {
  function checkContactExists(room) {
    if (!room.dialog) {
      return false;
    }
    const participantPublicKey = room.participants.find(participantPublicKey => {
      return participantPublicKey !== props.publicKey;
    });
    return !props.contacts.has(participantPublicKey);
  }

  return (
    <>
      {!props.roomsReady && (
        <Grow in={!props.ready}>
          <div className={props.classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
      )}
      {props.roomsReady && (
        <List>
          {[...props.rooms].map(([roomId, room]) =>
            (
              <RoomItem
                roomId={roomId}
                room={room}
                showDeleteButton={props.showDeleteButton}
                showAddContactButton={checkContactExists(room)}
                names={props.names}
                publicKey={props.publicKey}
                onAddContact={props.onAddContact}
                onRemoveContact={props.onRemoveContact}
                myName={props.myName}
              />
            )
          )}
        </List>
      )}
    </>
  );
}

export default withStyles(roomsListStyles)(
  withRouter(RoomsList)
);
