import * as React from "react";
import TextField from "@material-ui/core/TextField";
import Checkbox from "@material-ui/core/Checkbox";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";
import {withStyles} from "@material-ui/core";
import todoItemStyles from "./todoItemStyles";

class TodoItem extends React.Component {
  state = {
    oldName: this.props.name,
    name: this.props.name,
    isDone: this.props.isDone
  };

  componentWillReceiveProps(nextProps) {
    if (nextProps.name !== this.state.name || nextProps.isDone !== this.state.isDone) {
      this.setState({
        name: nextProps.name,
        isDone: nextProps.isDone
      })
    }
  }

  handleDelete = () => this.props.onDeleteItem(this.state.name);

  handleStatusChange = () => {
    this.props.onChangeItemState(this.state.name);
    this.setState({isDone: !this.state.isDone});
  };

  handleNameChange = e => this.setState({name: e.target.value});

  handleNameSubmit = () => {
    this.props.onRenameItem(this.state.oldName, this.state.name);
    this.setState({oldName: this.state.name});
  };

  render() {
    return (
      <TextField
        value={this.state.name}
        fullWidth
        onChange={this.handleNameChange}
        onBlur={this.handleNameSubmit}
        InputProps={{
          classes: {
            root: this.props.classes.itemInput,
          },
          startAdornment: (
            <Checkbox
              checked={this.state.isDone}
              className={this.props.classes.checkbox}
              onChange={this.handleStatusChange}
            />
          ),
          endAdornment: (
            <IconButton
              className={this.props.classes.deleteIconButton}
              onClick={this.handleDelete}
            >
              <DeleteIcon/>
            </IconButton>
          )
        }}
      />
    );
  }
}

export default withStyles(todoItemStyles)(TodoItem);
