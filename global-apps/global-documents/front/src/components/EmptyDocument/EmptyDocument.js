import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import Typography from "@material-ui/core/Typography";
import emptyDocumentStyles from "./emptyDocumentStyles";

function EmptyDocument({classes}) {
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Paper className={classes.paper}>
        <Typography className={classes.typography} variant="h6">
          <div className={classes.startDocument}>
            Please select a document or create new
          </div>
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptyDocumentStyles)(EmptyDocument);
