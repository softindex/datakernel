import React from 'react';
import SnackbarMaterial from '@material-ui/core/Snackbar';

class Snackbar extends React.Component {
  state = {
    error: this.props.error,
    isOpen: false
  };

  componentDidUpdate(prevProps, prevState) {
    if (prevProps.error !== this.props.error && Boolean(this.props.error)) {
      this.setState({
        error: this.props.error,
        isOpen: true
      });
    }
  }

  onClose = () => {
    this.setState({
      isOpen: false
    })
  };

  render() {
    return (
      <SnackbarMaterial
        {...this.props}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        autoHideDuration={5000}
        open={this.state.isOpen}
        message={this.props.error}
        onClose={this.onClose}
      />
    )
  }
}

export default Snackbar;
