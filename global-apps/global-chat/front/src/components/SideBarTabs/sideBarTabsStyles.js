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
        overflow: 'auto'
      },
      background: theme.palette.primary.contrastText,
      marginTop: theme.spacing(1),
      marginBottom: theme.spacing(1)
    },
    paperDivider: {
      background: theme.palette.primary.background,
      padding: theme.spacing(2),
      marginTop: theme.spacing(1),
      marginBottom: theme.spacing(1),
      boxShadow: 'none'
    },
    dividerText: {
      fontSize: '0.9rem'
    },
    secondaryDividerText: {
      textAlign: 'center',
      marginTop: theme.spacing(2)
    }
  }
};

export default sideBarTabsStyles;
