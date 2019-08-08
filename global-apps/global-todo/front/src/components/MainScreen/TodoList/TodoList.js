import ListContext from "../../../modules/list/ListContext";
import connectService from "../../../common/connectService";
import todoListStyles from "./todoListStyles";
import * as React from "react";
import withStyles from "@material-ui/core/es/styles/withStyles";
import TextField from "@material-ui/core/TextField";
import TodoItem from "./TodoItem/TodoItem";

class TodoList extends React.Component {
  constructor(props, context) {
    super(props, context);
    this.state = {
      newItemName: ''
    };
    this.handleSubmit = this.handleSubmit.bind(this);
    this.handleChange = this.handleChange.bind(this);
  }

  handleSubmit = e => {
    e.preventDefault();
    this.props.onCreateItem(this.state.newItemName);
    this.setState({newItemName: ''});
  };

  handleChange = e => {
    this.setState({
      newItemName: e.target.value
    })
  };

  render() {
    const {classes} = this.props;
    return (
      <div className={classes.root}>
        <div className={classes.headerPadding}>
          <form className={classes.textInput} onSubmit={this.handleSubmit}>
            <TextField
              id="outlined"
              className={classes.textField}
              value={this.state.newItemName}
              onChange={this.handleChange}
              margin="normal"
              variant="outlined"
            />
          </form>
          {Object.entries(this.props.items).map(item =>
            <TodoItem
              key={item[0]}
              name={item[0]}
              isDone={item[1]}
              onChangeItemState={this.props.onChangeItemState}
              onDeleteItem={this.props.onDeleteItem}
              onRenameItem={this.props.onRenameItem}
            />
          )}
        </div>
      </div>
    );
  }
}

export default connectService(ListContext,
  ({items, ready}, listService) => ({
    ready,
    items,

    onCreateItem(name) {
      listService.createItem(name);
    },

    onDeleteItem(name) {
      listService.deleteItem(name);
    },

    onRenameItem(name, newName) {
      listService.renameItem(name, newName);
    },

    onChangeItemState(name) {
      listService.changeItemState(name);
    }

  }))(
  withStyles(todoListStyles)(TodoList)
);
