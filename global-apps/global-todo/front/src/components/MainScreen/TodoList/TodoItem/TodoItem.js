import * as React from "react";
import TextField from "@material-ui/core/TextField";
import Checkbox from "@material-ui/core/Checkbox";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";

class TodoItem extends React.Component {
  constructor(props, context) {
    super(props, context);
    this.state = {
      oldName: this.props.name,
      name: this.props.name,
      isDone: this.props.isDone
    };
    this.handleDelete = this.handleDelete.bind(this);
    this.handleNameSubmit = this.handleNameSubmit.bind(this);
    this.handleNameChange = this.handleNameChange.bind(this);
    this.handleStatusChange = this.handleStatusChange.bind(this);
  }

  handleDelete = () => this.props.onDeleteItem(this.state.name);

  handleStatusChange = () => {
    this.props.onChangeItemState(this.state.name);
    this.setState({isDone: !this.state.isDone})
  };

  handleNameChange = e => this.setState({name: e.target.value});

  handleNameSubmit = () => {
    this.props.onRenameItem(this.state.oldName, this.state.name);
    this.setState({oldName: this.state.name});
  };

  render() {
    return (
      <div>
        <TextField
          id="outlined"
          value={this.state.name}
          onChange={this.handleNameChange}
          onBlur={this.handleNameSubmit}
          margin="normal"
          variant="outlined"
        />
        <Checkbox
          checked={this.state.isDone}
          onChange={this.handleStatusChange}
        />
        <IconButton onClick={this.handleDelete}>
          <DeleteIcon />
        </IconButton>
      </div>
    );
  }
}

export default TodoItem;
