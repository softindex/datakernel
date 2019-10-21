const sideBarMenuStyles = theme => {
  return {
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
    listTypography: {
      fontWeight: 'inherit',
      color: 'inherit'
    },
    foldersLink: {
      textDecoration: 'none',
      color: theme.palette.secondary.contrastText
    }
  }
};

export default sideBarMenuStyles;

