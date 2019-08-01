import React from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import roomItemStyles from "./roomItemStyles";
import ContactMenu from "../ContactMenu/ContactMenu";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import {Link, withRouter} from "react-router-dom";
import Badge from "@material-ui/core/Badge";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";
import {getAvatarLetters, getRoomName} from "../../common/utils";

class RoomItem extends React.Component {
  state = {
    showAddContactDialog: false
  };

  static defaultProps = {
    selected: false,
    isContactsTab: false,
    roomSelected: true
  };

  componentDidMount() {
    this.checkContactExists(this.props.room)
  }

  getRoomPath(roomId) {
    return path.join('/room', roomId || '');
  }

  onClickAddContact() {
    this.setState({
      showAddContactDialog: true
    });
  }

  checkContactExists(room) {
    if (room.participants.length === 2 && room.dialog) {
      const participantPublicKey = room.participants
        .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
      if (!this.props.contacts.has(participantPublicKey)) {
        return true
      }
    }
    return false;
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

  render() {
    const {classes, room, roomId} = this.props;
    return (
      <>
        <ListItem
          onClick={this.props.onClick}
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
                  {getAvatarLetters(getRoomName(room.participants, this.props.contacts,
                    this.props.publicKey)).toUpperCase()}
                  </Avatar>
              </Badge>
            </ListItemAvatar>

            <ListItemText
              primary={getRoomName(room.participants, this.props.contacts, this.props.publicKey)}
              className={classes.itemText}
              classes={{primary: classes.itemTextPrimary}}
            />
          </Link>
          {this.checkContactExists(room) && !this.props.isContactsTab && (
            <ContactMenu onAddContact={this.onClickAddContact.bind(this)}/>
          )}
          {this.props.isContactsTab && (
            <IconButton className={classes.deleteIcon}>
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
          onAddContact={this.props.onAddContact}
        />
      </>
    )
  }
}

export default withRouter(withStyles(roomItemStyles)(RoomItem));

