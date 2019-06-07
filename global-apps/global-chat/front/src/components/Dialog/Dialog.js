import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "../DialogsForms/formStyles";
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import MUDialog from '@material-ui/core/Dialog';

class Dialog extends React.Component {
  render() {
    const {children, onClose, classes, ...otherProps} = this.props;
    return (
      <MUDialog {...otherProps} onclose={onClose}>
        <IconButton
          aria-label="Close"
          className={classes.closeButton}
          onClick={this.props.onClose}
        >
          <CloseIcon/>
        </IconButton>
        {children}
      </MUDialog>
    );
  }
}

export default withStyles(formStyles)(Dialog);
