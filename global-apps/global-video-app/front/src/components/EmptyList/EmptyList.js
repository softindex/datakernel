import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import Typography from "@material-ui/core/Typography";
import emptyListStyles from "./emptyListStyles";

function EmptyList(props) {
  const classes = props.classes;
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Paper className={classes.paper}>
        <Typography className={classes.typography} variant="h6">
          <div className={classes.startMessage}>
            No videos has been uploaded yet
          </div>
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptyListStyles)(EmptyList);
