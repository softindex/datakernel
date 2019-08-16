import React from "react";
import {withStyles} from '@material-ui/core';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import MUDialog from '@material-ui/core/Dialog';
import CircularProgress from "@material-ui/core/CircularProgress";
import dialogStyles from "./dialogStyles";

function Dialog({children, onClose, loading, classes, maxWidth, ...otherProps}) {
  return (
    <MUDialog
      {...otherProps}
      onClose={onClose}
      classes={{paper: classes.muDialog}}
      maxWidth={maxWidth}
      fullWidth={maxWidth}
    >
      <IconButton
        className={classes.closeButton}
        onClick={onClose}
      >
        <CloseIcon/>
      </IconButton>
      {children}
      {loading && (
        <CircularProgress
          size={24}
          className={classes.circularProgress}
        />
      )}
    </MUDialog>
  );
}

export default withStyles(dialogStyles)(Dialog);
