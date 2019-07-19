import React from "react";
import {withStyles} from '@material-ui/core';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import MUDialog from '@material-ui/core/Dialog';
import CircularProgress from "@material-ui/core/CircularProgress";
import dialogStyles from "./dialogStyles";

class Dialog extends React.Component {
  render() {
    const {children, onClose, loading, classes, ...otherProps} = this.props;
    return (
      <MUDialog {...otherProps} onClose={onClose}>
        <IconButton
          aria-label="Close"
          className={classes.closeButton}
          onClick={this.props.onClose}
        >
          <CloseIcon/>
        </IconButton>
        {children}
        {loading  && (
          <CircularProgress
            size={24}
            className={classes.circularProgress}
          />
        )}
      </MUDialog>
    );
  }
}

export default withStyles(dialogStyles)(Dialog);
