const sideBarTabsStyles = theme => {
  return {
    paper: {
      boxShadow: `0px 6px  9px -5px rgba(0,0,0,0.2)`,
      background: theme.palette.primary.background
    },
    chatsList: {
      flexGrow: 1,
      overflow: 'hidden',
      '&:hover': {
        overflow: 'overlay'
      },
      background: theme.palette.primary.contrastText,
      marginTop: theme.spacing.unit,
      marginBottom: theme.spacing.unit
    },
    paperDivider: {
      background: theme.palette.primary.background,
      padding: theme.spacing.unit * 2,
      marginTop: theme.spacing.unit,
      marginBottom: theme.spacing.unit,
      boxShadow: 'none'
    },
    dividerText: {
      fontSize: '0.9rem'
    },
    secondaryDividerText: {
      textAlign: 'center',
      marginTop: theme.spacing.unit * 2
    }
  }
};

export default sideBarTabsStyles;