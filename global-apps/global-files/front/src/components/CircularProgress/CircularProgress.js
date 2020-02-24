import React from 'react';
import ErrorIcon from '@material-ui/icons/ErrorOutline';
import CompleteIcon from '@material-ui/icons/CheckCircleOutline';
import CircularProgressMaterial from '@material-ui/core/CircularProgress';

function CircularProgress(props) {
  const {
    variant = 'static',
    size = 24,
  } = props;

  if (props.isError) {
    return <ErrorIcon color="error"/>;
  }

  if (props.success) {
    return <CompleteIcon/>;
  }

  return (
    <CircularProgressMaterial
      {...props}
      variant={variant}
      size={size}
    />
  )
}

export default CircularProgress;
