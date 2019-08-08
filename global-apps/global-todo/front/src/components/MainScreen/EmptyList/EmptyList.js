import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import emptyListStyles from './emptyListStyles';
import Typography from "@material-ui/core/Typography";

class EmptyList extends React.Component {
  render() {
    let classes = this.props.classes;
    return (
      <div className={classes.root}>
        <div className={classes.headerPadding}/>
        <Paper className={classes.paper}>
          <Typography className={classes.typography} variant="h6">
            <div className={classes.startMessage}>
              Please select a list or create new
            </div>
          </Typography>
        </Paper>
      </div>
    );
  }
}

export default withStyles(emptyListStyles)(EmptyList);
