import React from "react";
import {withStyles} from '@material-ui/core';
import RoomItem from "../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

function RoomsList(props) {
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
                contacts={props.contacts}
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

export default withStyles(roomsListStyles)(RoomsList);
