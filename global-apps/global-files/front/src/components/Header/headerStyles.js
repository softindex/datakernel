const headerStyles = theme => {
  return {
    root: {
      boxShadow: 'none',
      backgroundColor: theme.palette.background.default
    },
    logo: {
      [theme.breakpoints.down('md')]: {
        display: 'none'
      },
    },
    grow: {
      flexGrow: 1
    },
    drawerTrigger: {
      display: 'none',
      marginRight: theme.spacing(2),
      [theme.breakpoints.down('md')]: {
        display: 'block'
      },
    },
    listItemIcon: {
      marginRight: theme.spacing(2),
      minWidth: 0
    },
    litItemText: {
      padding: `0px ${theme.spacing(2)}px`
    }
  };
};

export default headerStyles;
