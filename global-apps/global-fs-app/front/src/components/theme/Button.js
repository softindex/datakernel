import React, {Component} from 'react';
import MaterialButton from '@material-ui/core/Button';
import CircularProgress from '@material-ui/core/CircularProgress';
import {withStyles} from "@material-ui/core";
import classNames from 'classnames';

const buttonStyles = theme => ({
  rounded: {
    borderRadius: theme.spacing.unit * 10
  }
});

class Button extends Component {
  state = {
    isLoading: false
  };

  onClick = async () => {
    if (!this.props.onClick || this.state.isLoading || this.props.loading) {
      return;
    }

    const promise = this.props.onClick();

    if (!(promise instanceof Promise)) {
      return;
    }

    this.setState({
      isLoading: true
    });

    try {
      await promise;
    } finally {
      this.setState({isLoading: false});
    }
  };

  render() {
    const classes = classNames({
      [this.props.classes.root]: true,
      [this.props.className]: true,
      [this.props.classes.rounded]: this.props.shape === 'round'
    });
    const loading = this.props.loading || this.state.isLoading;

    return (
      <MaterialButton
        {...this.props}
        onClick={this.onClick}
        className={classes}
      >
        {!loading && this.props.children}
        {loading && (<CircularProgress color="inherit" size={22}/>)}
      </MaterialButton>
    );
  }
}

export default withStyles(buttonStyles)(Button);
