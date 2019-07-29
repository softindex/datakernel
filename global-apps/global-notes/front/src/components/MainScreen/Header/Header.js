import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import IconButton from '@material-ui/core/IconButton';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight';
import Icon from '@material-ui/core/Icon';
import headerStyles from './headerStyles';
import connectService from '../../../common/connectService';
import AccountContext from '../../../modules/account/AccountContext';
import NotesContext from '../../../modules/notes/NotesContext';

function Header({classes, notes, noteId, logout}) {
  const note = notes[noteId];

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
        <IconButton color="inherit" onClick={logout}>
          <Icon className={classes.accountIcon}>logout</Icon>
        </IconButton>
      </Toolbar>
    </AppBar>
  );
}

export default connectService(
  NotesContext,
  ({ready, notes}, notesService) => ({ready, notes, notesService})
)(
  connectService(
    AccountContext,
    (state, accountService) => ({
      logout() {
        accountService.logout();
      }
    })
  )(
    withStyles(headerStyles)(Header)
  )
);
