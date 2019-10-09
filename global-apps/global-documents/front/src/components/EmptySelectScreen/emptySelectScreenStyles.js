const emptyChatScreenStyles = theme => {
  return {
    root: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      height: theme.spacing(37)
    },
    paper: {
      display: 'flex',
      padding: `${theme.spacing(1)}px ${theme.spacing(2)}px`,
      boxShadow: 'none'
    }
  }
};

export default emptyChatScreenStyles;
