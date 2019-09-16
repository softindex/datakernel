import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import emptyDocumentScreenStyles from './emptyDocumentScreenStyles';
import Typography from "@material-ui/core/Typography";

function EmptyDocumentScreen({classes}) {
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Paper className={classes.paper}>
        <Typography className={classes.typography} variant="h6">
          <div className={classes.startMessage}>
            Please select a document to start editing
          </div>
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptyDocumentScreenStyles)(EmptyDocumentScreen);
