const sideBarStyles = theme => {
  return {
    wrapper: {
      boxShadow: `2px 0px 1px -2px rgba(0,0,0,0.2)`,
      background: theme.palette.primary.contrastText,
      width: 350,
      height: '100vh',
      display: 'flex',
      flexDirection: 'column',
      flexGrow: 0,
      flexShrink: 0
    },
    search: {
      marginTop: theme.spacing(9),
      display: 'flex',
      alignItems: 'center',
      boxShadow: '0px 2px 8px 0px rgba(0,0,0,0.1)',
      background: theme.palette.primary.background,
      border: 'none',
      flexGrow: 0,
      padding: theme.spacing(1),
      marginBottom: theme.spacing(1)
    },
    documentsList: {
      flexGrow: 1,
      overflow: 'hidden',
      '&:hover': {
        overflowY: 'auto'
      },
      background: theme.palette.primary.contrastText,
      marginBottom: theme.spacing(1)
    },
    secondaryText: {
      textAlign: 'center',
      marginTop: theme.spacing(1)
    }
  }
};

export default sideBarStyles;
