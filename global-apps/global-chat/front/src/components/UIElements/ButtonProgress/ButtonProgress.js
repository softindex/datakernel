import React from "react";
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import CircularProgress from "@material-ui/core/CircularProgress";
import buttonProgressStyles from "./buttonProgressStyles";

class ButtonWithProgress extends React.Component {
  state = {
    loading: false
  };

  onClick = () => {
    if (!this.props.onClick) {
      return;
    }

    const promise = this.props.onClick();
    if (promise) {
      this.setState({loading: true});
      promise.finally(() => {
        this.setState({loading: false});
      });
    }
  };

  render() {
    let {classes, disabled, children, loading, ...otherProps} = this.props;
    loading = loading || this.state.loading;
    return (
      <div className={classes.wrapper}>
        <Button
          {...otherProps}
          disabled={disabled || loading}
          onClick={this.onClick}
        >
          {children}
        </Button>
        {loading  && (
          <CircularProgress
            size={24}
            className={classes.buttonProgress}
          />
        )}
      </div>
    );
  }
}

export default withStyles(buttonProgressStyles)(ButtonWithProgress);
