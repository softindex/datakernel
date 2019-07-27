import React from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import roomItemStyles from "./roomItemStyles";
import SimpleMenu from "../SimpleMenu/SimpleMenu";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import {Link, withRouter} from "react-router-dom";
import Badge from "@material-ui/core/Badge";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";
import {getAvatarLetters, getRoomName, toEmoji} from "../../common/utils";

class RoomItem extends React.Component {
  state = {
    hover: false,
    showAddContactDialog: false,
    showMenuIcon: true
  };

  static defaultProps = {
    selected: false,
    showDeleteButton: false,
    roomSelected: true
  };

  componentDidMount() {
    this.checkContactExists(this.props.room)
  }

  getRoomPath = (roomId) => {
    return path.join('/room', roomId || '');
  };

  onClickAddContact() {
    this.setState({
      showAddContactDialog: true,
      showMenuIcon: false
    });
  }

  checkContactExists(room) {
    if (room.participants.length === 2) {
      const participantPublicKey = room.participants
        .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
      if (this.props.contacts.has(participantPublicKey)) {
        this.setState({
          showMenuIcon: false
        });
      }
    } else {
      this.setState({
        showMenuIcon: false
      })
    }
  }

  closeAddDialog = () => {
    this.setState({
      showAddContactDialog: false
    });
  };

  getContactId(room) {
    return room.participants
      .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
  }

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, room, roomId} = this.props;
    return (
      <>
        <ListItem
          onClick={this.props.onClick}
          onMouseEnter={this.toggleHover}
          onMouseLeave={this.toggleHover}
          className={classes.listItem}
          button
          selected={roomId === this.props.match.params.roomId && this.props.roomSelected}
        >
          <Link
            to={this.getRoomPath(roomId)}
            style={this.props.linkDisabled ? {pointerEvents: "none"} : null}
            className={classes.link}
          >
            <ListItemAvatar className={classes.avatar}>
              <Badge
                className={classes.badge}
                invisible={!this.props.selected}
                color="primary"
                variant="dot"
              >
                <Avatar className={classes.avatarContent}>
                  {getAvatarLetters(getRoomName(this.props.room.participants, this.props.contacts,
                    this.props.publicKey)).toUpperCase()}
                </Avatar>
              </Badge>
            </ListItemAvatar>

            <ListItemText
              primary={getRoomName(this.props.room.participants, this.props.contacts, this.props.publicKey)}
              className={classes.itemText}
              classes={{
                primary: classes.itemTextPrimary
              }}
            />
          </Link>
          {this.state.hover && this.state.showMenuIcon && (
            <SimpleMenu
              className={classes.menu}
              onAddContact={this.onClickAddContact.bind(this)}
              onDelete={this.props.onRemoveContact}
            />
          )}
          {this.state.hover && this.props.showDeleteButton && (
            <IconButton
              className={classes.deleteIcon}
              aria-label="Delete"
            >
              <DeleteIcon
                onClick={this.props.onRemoveContact}
                fontSize="medium"
              />
            </IconButton>
          )}
        </ListItem>
        <AddContactDialog
          open={this.state.showAddContactDialog}
          onClose={this.closeAddDialog}
          contactPublicKey={this.getContactId(room)}
          publicKey={this.props.publicKey}
          onAddContact={this.props.addContact}
        />
      </>
    )
  }
}

export default withRouter(withStyles(roomItemStyles)(RoomItem));

