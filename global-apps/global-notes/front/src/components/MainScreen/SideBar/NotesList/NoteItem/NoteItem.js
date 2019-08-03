import React from 'react';
import {Link, withRouter} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import ListItemText from '@material-ui/core/ListItemText';
import ListItem from '@material-ui/core/ListItem';
import noteItemStyles from './noteItemStyles';
import NoteMenu from '../../NoteMenu/NoteMenu';

function NoteItem({classes, noteId, noteName, getNotePath, onRename, onDelete, ...otherProps}) {
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
      <NoteMenu
        className={classes.menu}
        onRename={() => onRename(noteId, noteName)}
        onDelete={() => onDelete(noteId)}
      />
    </ListItem>
  );
}

export default withRouter(withStyles(noteItemStyles)(NoteItem));

