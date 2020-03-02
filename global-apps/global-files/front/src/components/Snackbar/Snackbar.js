import React, {useEffect, useState} from 'react';
import SnackbarMaterial from '@material-ui/core/Snackbar';

function Snackbar({error, ...otherProps}) {
  const [errorMessage, setErrorMessage] = useState(false);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    if (error) {
      setErrorMessage(error);
      setIsOpen(true);
    }
  }, [error]);

  const onClose = () => {
    setIsOpen(false);
  };

  return (
    <SnackbarMaterial
      {...otherProps}
      anchorOrigin={{
        vertical: 'top',
        horizontal: 'right',
      }}
      autoHideDuration={5000}
      open={isOpen}
      message={errorMessage}
      onClose={onClose}
    />
  )
}

export default Snackbar;
