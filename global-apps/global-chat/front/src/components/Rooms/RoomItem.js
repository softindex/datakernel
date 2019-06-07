import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import roomItemStyles from "./roomItemStyles";
import {Link} from 'react-router-dom';

class RoomItem extends React.Component {
  constructor(){
    super();
    this.quitRoom = this.quitRoom.bind(this);
  }

  quitRoom(){
    this.props.roomsService.quitRoom(this.props.room.id);
  }

  render() {
    const {classes, room} = this.props;
    const roomPath = path.join('/room', room.id || '');

    return (
      <Link to={roomPath} className={classes.link}>
        <ListItem className={classes.listItem} button>
          <ListItemAvatar>
            <Avatar/>
          </ListItemAvatar>
          <ListItemText primary={room.name}/>
        </ListItem>
      </Link>
    )
  }
}

export default withStyles(roomItemStyles)(RoomItem);
