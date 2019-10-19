import React from 'react';
import ErrorIcon from '@material-ui/icons/ErrorOutline';
import CompleteIcon from '@material-ui/icons/CheckCircleOutline';
import CircularProgressMaterial from '@material-ui/core/CircularProgress';

function CircularProgress(props) {
  const {
    variant = 'static',
    size = 24,
  } = props;

  return (
    <>
      {props.isError && (
        <ErrorIcon color="error"/>
      )}
      {props.success && <CompleteIcon/>}
      {!props.isError && !props.success && (
        <CircularProgressMaterial
          {...props}
          variant={variant}
          size={size}
        />
      )}
    </>
  )
}

export default CircularProgress;
