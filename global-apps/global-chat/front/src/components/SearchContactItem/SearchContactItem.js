import React from "react";
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import searchContactItemStyles from "./searchContactItemStyles";
import {withRouter} from "react-router-dom";
import {getAvatarLetters, createDialogRoomId} from "global-apps-common";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";
import Badge from "@material-ui/core/Badge";

class SearchContactItem extends React.Component {
  state = {
    showAddContactDialog: false
  };

  getAvatarLetters(contact) {
    if (contact.firstName !== '' && contact.lastName !== '') {
      return getAvatarLetters(contact.firstName + ' ' + contact.lastName);
    }
    return getAvatarLetters(contact.username);
  }

  onContactClick() {
    this.setState({
      showAddContactDialog: true
    });
  }

  onClickAddContact = () => {
    this.props.onAddContact(this.props.contactId);
    this.onCloseAddContactDialog();
  };

  onCloseAddContactDialog = () => {
    this.setState({
      showAddContactDialog: false
    });
  };

  render() {
    const {classes, contactId, contact} = this.props;
    return (
      <>
        <ListItem
          onClick={this.props.onClick || this.onContactClick.bind(this)}
          className={classes.listItem}
          button
          selected={createDialogRoomId(this.props.publicKey, contactId) === this.props.match.params.roomId}
        >
          <ListItemAvatar className={classes.avatar}>
            <Badge
              invisible={!this.props.selected}
              color="primary"
              variant="dot"
            >
              <Avatar className={classes.avatarContent}>
                {this.getAvatarLetters(contact).toUpperCase()}
              </Avatar>
            </Badge>
          </ListItemAvatar>
          <ListItemText
            primary={contact.username}
            secondary={contact.firstName + ' ' + contact.lastName}
            className={classes.itemText}
            classes={{
              primary: classes.itemTextPrimary
            }}
          />
        </ListItem>
        <ConfirmDialog
          open={this.state.showAddContactDialog}
          onClose={this.onCloseAddContactDialog}
          title="Add Contact"
          subtitle="Do you want to add this contact?"
          onConfirm={this.onClickAddContact}
        />
      </>
    )
  }
}

export default withRouter(withStyles(searchContactItemStyles)(SearchContactItem));


