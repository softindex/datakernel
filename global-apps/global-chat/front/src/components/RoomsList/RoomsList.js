import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import {withRouter} from "react-router-dom";

function RoomsList({
                     classes,
                     publicKey,
                     roomsReady,
                     rooms,
                     showDeleteButton,
                     onRemoveContact,
                     names,
                     namesReady,
                     contacts
                   }) {
  function checkContactExists(room) {
    if (!room.dialog) {
      return false;
    }
    const participantPublicKey = room.participants.find(participantPublicKey => {
      return participantPublicKey !== publicKey;
    });
    return !contacts.has(participantPublicKey);
  }

  return (
    <>
      {(!roomsReady || !namesReady) && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {roomsReady && namesReady && (
        <List>
          {[...rooms].map(([roomId, room]) =>
            (
              <RoomItem
                roomId={roomId}
                room={room}
                showDeleteButton={showDeleteButton}
                showAddContactButton={checkContactExists(room)}
                names={names}
                publicKey={publicKey}
                onRemoveContact={onRemoveContact}
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
