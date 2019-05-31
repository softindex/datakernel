import React from "react";
import path from "path";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";

class RoomItem extends React.Component {
  constructor(){
    super();
    this.quitRoom = this.quitRoom.bind(this);
  }

  quitRoom(){
    this.props.roomsService.quitRoom(this.props.room.id);
  }

  render() {
    let roomID = this.props.room.id;

    return <li>
      <a href={path.join('/room', roomID)}>
        {roomID.substring(0, 8)}
      </a>
      <button onClick={this.quitRoom}>
        Quit
      </button>
    </li>;
  }
}

export default RoomItem;
