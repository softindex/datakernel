import ListContext from "../../modules/list/ListContext";
import connectService from "../../common/connectService";
import todoListStyles from "./todoListStyles";
import * as React from "react";
import withStyles from "@material-ui/core/es/styles/withStyles";
import TextField from "@material-ui/core/TextField";
import TodoItem from "../TodoItem/TodoItem";
import {Paper, Button} from "@material-ui/core";
import Typography from "@material-ui/core/Typography";
import DoneAllIcon from '@material-ui/icons/DoneAll';

class TodoList extends React.Component {
  constructor(props, context) {
    super(props, context);
    this.state = {
      newItemName: ''
    };
  }

  onSubmit = e => {
    e.preventDefault();
    this.props.onCreateItem(this.state.newItemName);
    this.setState({newItemName: ''});
  };

  onItemChange = e => {
    this.setState({
      newItemName: e.target.value
    })
  };

  render() {
    const {classes} = this.props;
    let counter = 0;
    return (
      <Paper className={classes.paper}>
        <form onSubmit={this.onSubmit}>
          <TextField
            className={classes.textField}
            autoFocus
            placeholder="What needs to be done?"
            value={this.state.newItemName}
            onChange={this.onItemChange}
            variant="outlined"
            InputProps={{
              classes: {
                root: this.props.classes.itemInput,
              },
              startAdornment: (
                <div className={classes.iconButton}>
                  <DoneAllIcon/>
                </div>
              )
            }}
          />
        </form>
        {Object.entries(this.props.items).map(item => {
          if (!item[1]) {
            counter++;
          }
          return (
            <TodoItem
              name={item[0]}
              isDone={item[1]}
              onChangeItemState={this.props.onChangeItemState}
              onDeleteItem={this.props.onDeleteItem}
              onRenameItem={this.props.onRenameItem}
            />
          )
        })}
        {Object.entries(this.props.items).length !== 0 && (
          <div className={classes.listCaption}>
            <Typography
              variant="subtitle2"
              className={classes.captionCounter}
              color="textSecondary"
            >
              {counter} items left
            </Typography>
            <Button className={classes.captionButton} variant="outlined">All</Button>
            <Button className={classes.captionButton}>Active</Button>
            <Button className={classes.captionButton}>Completed</Button>
            <Button className={classes.captionButton}>Clear completed</Button>
          </div>
        )}
      </Paper>
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
