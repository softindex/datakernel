const sideBarStyles = theme => {
  return {
    root: {
      height: '100%',
      width: theme.spacing(32),
      backgroundColor: theme.palette.background.default,
      border: 'none',
      [theme.breakpoints.up('md')]: {
        position: 'relative'
      },
    },
    icon: {
      marginRight: theme.spacing(1)
    },
    fab: {
      backgroundColor: theme.palette.common.white,
      margin: `${theme.spacing(2)}px ${theme.spacing(4)}px`,
      boxShadow: `0 1px 2px 0 rgba(60,64,67,0.302), 0 1px 3px 1px rgba(60,64,67,0.149);`
    },
    listItem: {
      borderRadius: `0 ${theme.spacing(3)}px ${theme.spacing(3)}px 0`
    },
    menuList: {
      paddingRight: theme.spacing(3)
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
