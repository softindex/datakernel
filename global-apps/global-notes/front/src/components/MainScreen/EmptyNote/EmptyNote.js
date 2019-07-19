import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import emptyNoteStyles from './emptyNoteStyles';
import Typography from "@material-ui/core/Typography";

class EmptyNote extends React.Component {
  render() {
    let classes = this.props.classes;
    return (
      <div className={classes.root}>
        <div className={classes.headerPadding}/>
        <Paper className={classes.paper}>
          <Typography className={classes.typography} variant="h6">
            <div className={classes.startMessage}>
              Please select a note or create new
            </div>
          </Typography>
        </Paper>
      </div>
    );
  }
}

export default withStyles(emptyNoteStyles)(EmptyNote);
