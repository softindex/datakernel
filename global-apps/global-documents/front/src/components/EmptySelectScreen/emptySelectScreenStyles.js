const emptyChatScreenStyles = theme => {
  return {
    root: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: theme.spacing(14)
    },
    paper: {
      display: 'flex',
      padding: `${theme.spacing(1)}px ${theme.spacing(2)}px`,
      boxShadow: 'none'
    }
  }
};

export default emptyChatScreenStyles;
