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
      padding: `${theme.spacing.unit}px 0px`,
      display: 'flex',
      alignItems: 'center',
      boxShadow: 'none',
      border: 'none',
      flexGrow: 0,
      paddingBottom: theme.spacing.unit,
      marginTop: theme.spacing.unit * 8
    }
  }
};

export default sideBarStyles;