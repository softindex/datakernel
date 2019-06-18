import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import roomItemStyles from "./roomItemStyles";
import {Link} from 'react-router-dom';
import SimpleMenu from "../UIElements/SimpleMenu";

class RoomItem extends React.Component {
  constructor(){
    super();
    this.state = {
      hover: false
    };
    this.quitRoom = this.quitRoom.bind(this);
  }

  quitRoom(){
    this.props.quitRoom(this.props.room.id);
  }

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, room} = this.props;
    const roomPath = path.join('/room', room.id || '');

    return (
      <Link to={roomPath} className={classes.link}>
        <ListItem
          onMouseEnter={this.toggleHover}
          onMouseLeave={this.toggleHover}
          className={classes.listItem}
          button
        >
          <ListItemAvatar className={classes.avatar}>
            <Avatar/>
          </ListItemAvatar>
          <ListItemText primary={room.name}/>
          {this.state.hover && this.props.showMenuIcon && (
            <SimpleMenu className={classes.menu} onDelete={this.quitRoom}/>
          )}
        </ListItem>
      </Link>
    )
  }
}

export default withStyles(roomItemStyles)(RoomItem);
