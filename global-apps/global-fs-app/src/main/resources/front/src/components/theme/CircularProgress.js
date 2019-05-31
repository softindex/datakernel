import React from 'react';
import ErrorIcon from '@material-ui/icons/ErrorOutline';
import CompleteIcon from '@material-ui/icons/CheckCircleOutline';
import CircularProgressMaterial from '@material-ui/core/CircularProgress';

class CircularProgress extends React.Component {
  render() {
    const {
      variant = 'static',
      size = 24,
    } = this.props;

    if (this.props.isError) {
      return <ErrorIcon color="error"/>
    }

    if (this.props.success) {
      return <CompleteIcon/>
    }

    return (
      <CircularProgressMaterial
        {...this.props}
        variant={variant}
        size={size}
      />
    )
  }
}

export default CircularProgress;
