import React from 'react';
import {Button, withStyles} from "@material-ui/core";
import Typography from "@material-ui/core/Typography";
import todoFooterStyles from "./todoFooterStyles";

function TodoFooter({classes, selected, items, getAmountUncompletedTodo, onSelectedChange, onClearCompleted}) {
  return (
    <>
      {items.size !== 0 && (
        <div className={classes.listCaption}>
          <Typography
            variant="subtitle2"
            className={classes.captionCounter}
            color="textSecondary"
          >
            {getAmountUncompletedTodo()} items left
          </Typography>
          <Button
            className={classes.captionButton}
            variant={selected === 'all' ? "outlined" : null}
            onClick={onSelectedChange.bind(this, 'all')}
          >
            All
          </Button>
          <Button
            className={classes.captionButton}
            variant={selected === 'active' ? "outlined" : null}
            onClick={onSelectedChange.bind(this, 'active')}
          >
            Active
          </Button>
          <Button
            className={classes.captionButton}
            variant={selected === 'completed' ? "outlined" : null}
            onClick={onSelectedChange.bind(this, 'completed')}
          >
            Completed
          </Button>
          <Button
            className={classes.captionButton}
            onClick={onClearCompleted}
          >
            Clear completed
          </Button>
        </div>
      )}
    </>
  )
}

export default withStyles(todoFooterStyles)(TodoFooter);