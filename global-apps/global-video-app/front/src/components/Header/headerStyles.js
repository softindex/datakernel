const headerStyles = (theme) => ({
  appBar: {
    position: 'static',
    zIndex: theme.zIndex.drawer + 1,
    height: 65
  },
  title: {
    marginLeft: theme.spacing(3),
  },
  iconButton: {
    '&:hover': {
      cursor: 'pointer'
    },
    color: 'inherit'
  },
  listItemIcon: {
    color: 'inherit'
  },
  arrowIcon: {
    fontSize: '1.25rem'
  },
  drawer: {
    width: 355
  },
  list: {
    width: 355
  },
  menuItemIcon: {
    fontSize: theme.spacing(3)
  }
});

export default headerStyles;
