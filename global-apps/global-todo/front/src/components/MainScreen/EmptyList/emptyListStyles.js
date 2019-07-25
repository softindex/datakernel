const emptyListStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      flexGrow: 1,
      height: '100vh',
      alignItems: 'center',
      justifyContent: 'center'
    },
    headerPadding: theme.mixins.toolbar,
    typography: {
      color: theme.palette.primary.contrastText,
    },
    paper: {
      display: 'flex',
      backgroundColor: '#808080',
      padding: 6,
      paddingLeft: `${theme.spacing.unit * 2}px`,
      paddingRight: `${theme.spacing.unit * 2}px`,
      borderRadius: 30,
      boxShadow: 'none'
    },
    startMessage: {
      fontSize: '1rem',
      textAlign: 'center',
    }
  }
};

export default emptyListStyles;
