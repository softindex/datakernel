import React from "react";
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import roomItemStyles from "./roomItemStyles";
import SimpleMenu from "../../SimpleMenu/SimpleMenu";
import AddContactForm from "../../../DialogsForms/AddContactForm";
import {Link, withRouter} from "react-router-dom";

class RoomItem extends React.Component {
  state = {
    hover: false,
    showAddContactDialog: false,
    contactExists: false
  };

  componentDidMount() {
    this.checkContactExists(this.props.room)
  }

  onClickAddContact() {
    this.setState({
      showAddContactDialog: true,
      contactExists: true
    });
  }

  checkContactExists(room) {
    if (room.participants.length === 2) {
      const participantPublicKey = room.participants
        .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
      if (this.props.contacts.get(participantPublicKey)) {
        this.setState({
          contactExists: true
        });
      }
    } else {
      this.setState({
        contactExists: true
      })
    }
  }

  closeAddDialog = () => {
    this.setState({
      hover: false,
      showAddContactDialog: false
    });
  };

  getContactId(room) {
    return  room.participants
      .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
  }

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, room} = this.props;
    return (
      <>
        <ListItem
          onMouseEnter={this.toggleHover}
          onMouseLeave={this.toggleHover}
          className={classes.listItem}
          button
          selected={this.props.roomId === this.props.match.params.roomId}
        >
          <Link
            to={this.props.getRoomPath(this.props.roomId)}
            onClick={this.props.onClickLink(this.props.roomId)}
            className={classes.link}
          >
            <ListItemAvatar className={classes.avatar}>
              <Avatar/>
            </ListItemAvatar>
            <ListItemText
              primary={room.name}
              className={classes.itemText}
              classes={{
                primary: classes.itemTextPrimary
              }}
            />
          </Link>

          {this.state.hover && this.props.showMenuIcon && !this.state.contactExists && (
            <SimpleMenu
              className={classes.menu}
              onAddContact={this.onClickAddContact.bind(this)}
              onDelete={this.props.quitRoom}
            />
          )}
        </ListItem>
        <AddContactForm
          open={this.state.showAddContactDialog}
          onClose={this.closeAddDialog}
          contactPublicKey={this.getContactId(room)}
        />
      </>
    )
  }
}

export default withRouter(withStyles(roomItemStyles)(RoomItem));

