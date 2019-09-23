import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import Typography from "@material-ui/core/Typography";
import emptyNoteStyles from "./emptyNoteStyles";

function EmptyNote({classes}) {
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Paper className={classes.paper}>
        <Typography className={classes.typography} variant="h6">
          <div className={classes.emptyNote}>
            Please select a document or create new
          </div>
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptyNoteStyles)(EmptyNote);
