import React from "react";
import {withStyles} from '@material-ui/core';
import {DialogTitle} from '@material-ui/core';
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogActions from "@material-ui/core/DialogActions";
import Button from "@material-ui/core/Button";
import Dialog from "../Dialog/Dialog";
import confirmDialogStyles from "./confirmDialogStyles";

function ConfirmDialog({open, onConfirm, onClose, title, subtitle, classes}) {
  return (
    <Dialog
      open={open}
      onClose={onClose}
    >
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <DialogContentText>
          {subtitle}
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button
          className={classes.actionButton}
          onClick={onClose}
          color="primary"
        >
          Cancel
        </Button>
        <Button
          className={classes.actionButton}
          onClick={onConfirm}
          color="primary"
          variant="contained"
        >
          OK
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default withStyles(confirmDialogStyles)(ConfirmDialog);
