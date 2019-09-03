import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import {DialogTitle} from '@material-ui/core';
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogActions from "@material-ui/core/DialogActions";
import Button from "@material-ui/core/Button";
import Dialog from "../Dialog/Dialog";
import confirmDialogStyles from "./confirmDialogStyles";
import CircularProgress from "@material-ui/core/CircularProgress";

function ConfirmDialog({classes, onClose, onConfirm, title, subtitle}) {
  const [loading, setLoading] = useState(false);

  function handleConfirm() {
    const promise = onConfirm();
    if (promise) {
      setLoading(true);
      promise.finally(() => setLoading(false));
    }
  }

  return (
    <Dialog onClose={onClose}>
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
          disabled={loading}
        >
          Cancel
        </Button>
        <Button
          className={classes.actionButton}
          onClick={handleConfirm}
          color="primary"
          variant="contained"
          disabled={loading}
        >
          OK
        </Button>
      </DialogActions>
      {loading && (
        <CircularProgress
          size={24}
          className={classes.circularProgress}
        />
      )}
    </Dialog>
  );
}

export default withStyles(confirmDialogStyles)(ConfirmDialog);

