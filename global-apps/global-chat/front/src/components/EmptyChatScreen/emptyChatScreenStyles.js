const emptyChatScreenStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      flexGrow: 1,
      alignItems: 'center',
      justifyContent: 'center'
    },
    headerPadding: theme.mixins.toolbar,
    typography: {
      color: theme.palette.primary.contrastText,
    },
    paper: {
      display: 'flex',
      backgroundColor: theme.palette.secondary.darkGrey,
      padding: `${theme.spacing(1)}px ${theme.spacing(2)}px`,
      borderRadius: 30,
      boxShadow: 'none'
    },
    startMessage: {
      fontSize: '1rem',
      textAlign: 'center',
    }
  }
};

export default emptyChatScreenStyles;
