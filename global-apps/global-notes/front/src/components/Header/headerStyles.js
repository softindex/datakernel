const headerStyles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: theme.spacing.unit * 8
  },
  noteTitleContainer: {
    display: 'flex',
    flexGrow: 1,
    overflow: 'hidden'
  },
  title: {
    marginLeft: theme.spacing.unit * 3,
    minWidth: theme.spacing.unit * 37
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
    fontSize: theme.spacing.unit * 3
  },
  logout: {
    marginLeft: theme.spacing.unit * 2,
    '&:hover': {
      cursor: 'pointer'
    }
  }
});

export default headerStyles;
