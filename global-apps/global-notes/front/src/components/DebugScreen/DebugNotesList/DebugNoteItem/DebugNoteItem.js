import React from "react";
import {withStyles} from '@material-ui/core';
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import noteItemStyles from "./noteItemStyles";
import {Link, withRouter} from "react-router-dom";

class DebugNoteItem extends React.Component {
  state = {hover: false};

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, noteId, noteName} = this.props;
    return (
      <ListItem
        onMouseEnter={this.toggleHover}
        onMouseLeave={this.toggleHover}
        className={classes.listItem}
        button
        selected={noteId === this.props.match.params.noteId}
      >
        <Link
          to={this.props.getNotePath(noteId)}
          className={classes.link}
        >
          <ListItemText
            primary={noteName}
            className={classes.itemText}
            classes={{
              primary: classes.itemTextPrimary
            }}
          />
        </Link>
      </ListItem>
    )
  }
}

export default withRouter(withStyles(noteItemStyles)(DebugNoteItem));

