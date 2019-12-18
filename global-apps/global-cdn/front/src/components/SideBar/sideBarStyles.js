const sideBarStyles = theme => {
  return {
    root: {
      height: '100%',
      width: theme.spacing.unit * 32,
      backgroundColor: theme.palette.background.default,
      border: 'none',
      [theme.breakpoints.up('md')]: {
        position: 'relative'
      },
    },
    icon: {
      marginRight: theme.spacing.unit
    },
    fab: {
      backgroundColor: theme.palette.common.white,
      margin: `${theme.spacing.unit * 2}px ${theme.spacing.unit * 4}px`,
      boxShadow: `0 1px 2px 0 rgba(60,64,67,0.302), 0 1px 3px 1px rgba(60,64,67,0.149);`
    },
    listItem: {
      borderRadius: `0 ${theme.spacing.unit * 3}px ${theme.spacing.unit * 3}px 0`
    },
    menuList: {
      paddingRight: theme.spacing.unit * 3
    },
    listItemSelected: {
      fontWeight: '500',
      color: `${theme.palette.primary.main}`
    },
    uploadInput: {
      display: 'none'
    },
    listTypography: {
      fontWeight: 'inherit',
      color: 'inherit'
    }
  }
};

export default sideBarStyles;
