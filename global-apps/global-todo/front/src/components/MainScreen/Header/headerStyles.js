const headerStyles = (theme) => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: 65
  },
  listTitleContainer: {
    display: 'flex',
    marginLeft: theme.spacing.unit * 24.2,
    flexGrow: 1,
    flexDirection: 'row-reverse'
  },
  title: {
    marginLeft: theme.spacing.unit * 3,
  },
  listTitle: {
    fontSize: '1rem',
    flexGrow: 1,
    display: 'flex',
    alignItems: 'center'
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
  accountIcon: {
    fontSize: theme.spacing.unit * 3
  }
});

export default headerStyles;
