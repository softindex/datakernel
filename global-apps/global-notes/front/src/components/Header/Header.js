import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight';
import Icon from '@material-ui/core/Icon';
import headerStyles from './headerStyles';
import {connectService, AuthContext} from 'global-apps-common';
import {getInstance, useService} from "global-apps-common";
import NotesService from "../../modules/notes/NotesService";

function HeaderView({classes, note, onLogout}) {
  return (
    <AppBar className={classes.appBar} position="fixed">
      <Toolbar>
        <Typography
          color="inherit"
          variant="h6"
          className={classes.title}
        >
          Global Notes
        </Typography>
        <div className={classes.noteTitleContainer}>
          {note !== undefined && (
            <>
              <ListItemIcon className={classes.listItemIcon}>
                <ArrowIcon className={classes.arrowIcon}/>
              </ListItemIcon>
              <Typography
                className={classes.noteTitle}
                color="inherit"
              >
                {note}
              </Typography>
            </>
          )}
        </div>
        <div
          color="inherit"
          onClick={onLogout}
          className={classes.logout}
        >
          <Icon className={classes.accountIcon}>logout</Icon>
        </div>
      </Toolbar>
    </AppBar>
  );
}

function Header({classes, noteId, onLogout}) {
  const notesService = getInstance(NotesService);
  const {notes} = useService(notesService);
  const note = notes[noteId];
  const props = {
    classes,
    note,
    onLogout
  };

  return <HeaderView {...props}/>
}

export default connectService(
  AuthContext,
  (state, accountService) => ({
    onLogout() {
      accountService.logout();
    }
  })
)(
  withStyles(headerStyles)(Header)
);
