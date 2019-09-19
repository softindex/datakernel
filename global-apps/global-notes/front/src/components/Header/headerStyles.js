const headerStyles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: theme.spacing(8)
  },
  noteTitleContainer: {
    display: 'flex',
    flexGrow: 1,
    overflow: 'hidden'
  },
  title: {
    marginLeft: theme.spacing(3),
    minWidth: theme.spacing(37)
  },
  noteTitle: {
    fontSize: theme.typography.body1.fontSize,
    overflow: 'hidden',
    textOverflow: 'ellipsis'
  },
  listItemIcon: {
    color: 'inherit'
  },
  arrowIcon: {
    fontSize: theme.typography.h6.fontsize
  },
  accountIcon: {
    fontSize: theme.spacing(3)
  },
  logout: {
    marginLeft: theme.spacing(2),
    '&:hover': {
      cursor: 'pointer'
    }
  }
});

export default headerStyles;
