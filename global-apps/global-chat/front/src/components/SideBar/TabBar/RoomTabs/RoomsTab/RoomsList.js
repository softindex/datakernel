import React from "react";
import {withStyles} from '@material-ui/core';
import connectService from "../../../../../common/connectService";
import RoomsContext from "../../../../../modules/rooms/RoomsContext";
import RoomItem from "../../../../Rooms/RoomItem";
import roomsListStyles from "./roomsListStyles";
import List from "@material-ui/core/List";

class RoomsList extends React.Component {
  render() {
    const {classes, ready, rooms, roomsService} = this.props;
    return (
      <div className={classes.root}>
        {!ready ?
          <p>Loading...</p> :
          <div>
            <List>
              {rooms.map(value =>
                <RoomItem
                  room={value}
                  roomsService={roomsService}
                />
              )}
            </List>
          </div>
        }
      </div>
    );
  }
}

export default connectService(
  RoomsContext, ({ready, rooms}, roomsService) => ({roomsService, ready, rooms})
)(
  withStyles(roomsListStyles)(RoomsList)
);
