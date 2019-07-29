const headerStyles = theme => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: theme.spacing.unit * 8
  },
  noteTitleContainer: {
    display: 'flex',
    marginLeft: theme.spacing.unit * 24.2,
    flexGrow: 1
  },
  title: {
    marginLeft: theme.spacing.unit * 3,
  },
  noteTitle: {
    fontSize: theme.typography.body1.fontSize,
    flexGrow: 1,
    display: 'flex',
    alignItems: 'center'
  },
  listItemIcon: {
    color: 'inherit'
  },
  arrowIcon: {
    fontSize: theme.typography.h6.fontsize
  },
  accountIcon: {
    fontSize: theme.spacing.unit * 3
  }
});

export default headerStyles;
