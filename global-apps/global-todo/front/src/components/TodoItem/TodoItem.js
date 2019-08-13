import * as React from "react";
import TextField from "@material-ui/core/TextField";
import Checkbox from "@material-ui/core/Checkbox";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";
import {withStyles} from "@material-ui/core";
import todoItemStyles from "./todoItemStyles";

function TodoItem(props) {

  const onDelete = () => props
    .onDeleteItem(props.name);

  const onStatusChange = () => {
    props.onToggleItemStatus(props.name);
  };

  const onNameChange = event => props
    .onRenameItem(props.name, event.target.value);

  return (
    <TextField
      value={props.name}
      fullWidth
      onChange={event => onNameChange(event)}
      className={props.isDone ? props.classes.textField : null}
      InputProps={{
        classes: {
          root: props.classes.itemInput,
        },
        startAdornment: (
          <Checkbox
            checked={props.isDone}
            className={props.classes.checkbox}
            onChange={onStatusChange}
          />
        ),
        endAdornment: (
          <IconButton
            className={props.classes.deleteIconButton}
            onClick={onDelete}
          >
            <DeleteIcon/>
          </IconButton>
        )
      }}
    />
  );
}

export default withStyles(todoItemStyles)(TodoItem);
