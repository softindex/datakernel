const createFolderMenuStyles = theme => {
  return {
    uploadInput: {
      display: 'none'
    },
    listTypography: {
      fontWeight: 'inherit',
      color: 'inherit'
    },
    listItemIcon: {
      marginRight: theme.spacing(2),
      minWidth: 0
    },
    litItemText: {
      padding: `0px ${theme.spacing(2)}px`
    }
  }
};

export default createFolderMenuStyles;
