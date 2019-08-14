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
    paper: {
      boxShadow: `0px 6px  9px -5px rgba(0,0,0,0.2)`,
      background: theme.palette.primary.background
    },
    documentsList: {
      flexGrow: 1,
      overflowY: 'auto',
      background: theme.palette.primary.contrastText,
      marginTop: theme.spacing.unit,
      marginBottom: theme.spacing.unit
    },
    button: {
      width: theme.spacing.unit * 41,
      borderRadius: 70,
      marginBottom: theme.spacing.unit,
      marginTop: theme.spacing.unit
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
    },
    inputDiv: {
      marginLeft: theme.spacing.unit,
      marginRight: theme.spacing.unit * 3,
      flex: 1
    },
    input: {
      textOverflow: 'ellipsis',
      overflow: 'hidden',
      whiteSpace: 'nowrap'
    },
    iconButton: {
      padding: `${theme.spacing.unit}px ${theme.spacing.unit}px`
    },
    secondaryText: {
      textAlign: 'center',
      marginTop: theme.spacing.unit
    },
    paperDivider: {
      background: theme.palette.primary.background,
      padding: theme.spacing.unit * 2,
      marginTop: theme.spacing.unit,
      marginBottom: theme.spacing.unit,
      boxShadow: 'none'
    },
    paperError: {
      background: theme.palette.secondary.main,
      padding: theme.spacing.unit * 2,
      margin: theme.spacing.unit,
      borderRadius: theme.spacing.unit,
      boxShadow: 'none'
    },
    dividerText: {
      fontSize: '0.9rem'
    },
    progressWrapper: {
      marginLeft: theme.spacing.unit * 18,
      marginTop: theme.spacing.unit * 2
    },
    secondaryDividerText: {
      textAlign: 'center',
      marginTop: theme.spacing.unit * 2
    }
  }
};

export default sideBarStyles;
