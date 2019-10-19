import React from 'react';
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import CloseIcon from '@material-ui/icons/Close';
import List from "@material-ui/core/List";
import {withStyles} from "@material-ui/core";
import uploadingAlertStyles from "./uploadingAlertStyles";

function UploadingAlert({classes, items, uploads, onClose}) {
  setTimeout(() => {
    onClose();
  }, 4000);
  return (
    <Paper className={classes.root}>
      <div className={classes.header}>
        <Typography color="inherit" variant="subtitle1" className={classes.title}>
          {uploads.length} uploads complete
        </Typography>
        <IconButton color="inherit" onClick={onClose}>
          <CloseIcon/>
        </IconButton>
      </div>
      <div className={classes.body}>
        <List> {items} </List>
      </div>
    </Paper>
  );
}

export default withStyles(uploadingAlertStyles)(UploadingAlert);
