import React from 'react';
import {Link, withRouter} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import ListItemText from '@material-ui/core/ListItemText';
import ListItem from '@material-ui/core/ListItem';
import noteItemStyles from './noteItemStyles';

function DebugNoteItem({classes, noteId, noteName, getNotePath, ...otherProps}) {
  return (
    <ListItem
      className={classes.listItem}
      button
      selected={noteId === otherProps.match.params.noteId}
    >
      <Link
        to={getNotePath(noteId)}
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
  );
}

export default withRouter(withStyles(noteItemStyles)(DebugNoteItem));

