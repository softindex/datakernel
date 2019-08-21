import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

function RoomsList(props) {
  function onRemoveContact(room) {
    const publicKey = room.participants.find(publicKey => publicKey !== props.publicKey);
    const name = props.contacts.get(publicKey).name;
    return props.onRemoveContact(publicKey, name);
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
                isContactsTab={props.isContactsTab}
                contacts={props.contacts}
                names={props.names}
                publicKey={props.publicKey}
                onAddContact={props.onAddContact}
                onRemoveContact={onRemoveContact.bind(this, room)}
                myName={props.myName}
              />
            )
          )}
        </List>
      )}
    </>
  );
}

export default withStyles(roomsListStyles)(RoomsList);
