import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import emptySelectScreenStyles from './emptySelectScreenStyles';
import Typography from "@material-ui/core/Typography";

function EmptySelectScreen({classes}) {
  return (
    <div className={classes.root}>
      <Paper className={classes.paper}>
        <Typography
          color="textSecondary"
          variant="subtitle1"
        >
          Search people to add to new document
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptySelectScreenStyles)(EmptySelectScreen);
