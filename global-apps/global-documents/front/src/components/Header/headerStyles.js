const headerStyles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: 65
  },
  documentTitleContainer: {
    display: 'flex',
    marginLeft: theme.spacing(14),
    flexGrow: 1,
    flexDirection: 'row-reverse'
  },
  title: {
    marginLeft: theme.spacing(3),
  },
  documentTitle: {
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
  }
});

export default headerStyles;
