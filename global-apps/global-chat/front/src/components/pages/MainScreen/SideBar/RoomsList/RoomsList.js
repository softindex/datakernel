import React from "react";
import {withStyles} from '@material-ui/core';
import connectService from "../../../../../common/connectService";
import RoomsContext from "../../../../../modules/rooms/RoomsContext";
import RoomItem from "../../../../RoomItem/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class RoomsList extends React.Component {
  render() {
    const {classes, ready, rooms, roomsService, quitRoom} = this.props;
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
              {rooms.map(value =>
                <RoomItem
                  room={value}
                  quitRoom={quitRoom}
                  roomsService={roomsService}
                  showMenuIcon={true}
                />
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default connectService(
  RoomsContext, ({ready, rooms}, roomsService) => ({
    roomsService, ready, rooms,
    quitRoom(id) {
      return roomsService.quitRoom(id);
    }
  })
)(
  withStyles(roomsListStyles)(RoomsList)
);
