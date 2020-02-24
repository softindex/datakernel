import React, {useState} from "react";
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';

function PromptDialog({title, onClose, onSubmit}) {
  const [value, setValue] = useState('');

  const onChange = (event) => {
    setValue(event.currentTarget.value);
  };

  const onClickSubmit = () => {
    onSubmit(value);
  };

  return (
    <Dialog
      open={true}
      onClose={onClose}
    >
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          margin="dense"
          label="Folder name"
          type="text"
          onChange={onChange}
          fullWidth
        />
      </DialogContent>
      <DialogActions>
        <Button
          onClick={onClose}
          color="primary"
        >
          Cancel
        </Button>
        <Button
          onClick={onClickSubmit}
          color="primary"
          variant="contained"
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default PromptDialog;
