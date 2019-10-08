const emptyDocumentStyles = theme => {
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
      backgroundColor: theme.palette.grey[600],
      padding: 6,
      paddingLeft: theme.spacing(2),
      paddingRight: theme.spacing(2),
      borderRadius: 30,
      boxShadow: 'none'
    },
    emptyNote: {
      fontSize: '1rem',
      textAlign: 'center',
    }
  }
};

export default emptyDocumentStyles;
