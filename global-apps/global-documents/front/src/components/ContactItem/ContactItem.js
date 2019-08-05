import React from "react";
import {withStyles} from '@material-ui/core';
import contactItemStyles from "./contactItemStyles"
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import Badge from "@material-ui/core/Badge";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from '@material-ui/icons/Delete';
import {withRouter} from "react-router-dom";

class ContactItem extends React.Component {
  static defaultProps = {
    selected: false,
    showDeleteButton: false
  };

  render() {
    const {classes, name} = this.props;
    return (
        <ListItem
          onClick={this.props.onClick}
          className={classes.contactItem}
          button
          selected={this.props.documentId === this.props.match.params.documentId && this.props.showDeleteButton}
        >
          <Badge
            className={classes.badge}
            invisible={!this.props.selected}
            color="primary"
            variant="dot"
          >
            <ListItemAvatar className={classes.avatar}>
              <Avatar className={classes.avatarContent}>
                {name.includes(" ") ?
                  (name.charAt(0) + name.charAt(name.indexOf(" ") + 1)).toUpperCase() :
                  (name.charAt(0) + name.charAt(1)).toUpperCase()
                }
              </Avatar>
            </ListItemAvatar>
          </Badge>
          <ListItemText
            primary={name}
            className={classes.itemText}
            classes={{
              primary: classes.itemTextPrimary
            }}
          />
          {this.props.showDeleteButton && (
            <IconButton className={classes.deleteIcon}>
              <DeleteIcon
                onClick={this.props.onRemoveContact}
                fontSize="medium"
              />
            </IconButton>
          )}
        </ListItem>
    );
  }
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));
