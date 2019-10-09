const headerStyles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: 65
  },
  chatTitleContainer: {
    display: 'flex',
    flexGrow: 1,
    overflow: 'hidden'
  },
  title: {
    marginLeft: theme.spacing(3),
    minWidth: theme.spacing(37)
  },
  chatTitle: {
    flexGrow: 1,
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    display: 'flex'
  },
  iconButton: {
    '&:hover': {
      cursor: 'pointer'
    },
    color: 'inherit'
  },
  listItemIcon: {
    color: 'inherit',
    alignItems: 'center'
  },
  arrowIcon: {
    fontSize: '1.25rem'
  }
});

export default headerStyles;
