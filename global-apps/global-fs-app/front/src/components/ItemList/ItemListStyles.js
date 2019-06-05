const itemListStyles = theme => ({
  root: {
    height: '100%',
    maxWidth: '100%',
    display: 'flex',
    flexDirection: 'column',
    padding: theme.spacing.unit * 2,
    flexGrow: '1'
  },
  wrapper: {
    display: 'flex',
    flexGrow: '1',
    height: '100%',
    justifyContent: 'center',
    alignItems: 'center'
  },
  listWrapper: {
    display: 'flex',
    flexDirection: 'row',
    flexWrap: 'wrap'
  },
  section: {
    margin: `${theme.spacing.unit * 2}px 0`,
    flexDirection: 'column'
  },
  emptyIndicator: {
    padding: theme.spacing.unit * 2,
    width: theme.spacing.unit * 8,
    height: theme.spacing.unit * 8,
    backgroundColor: theme.palette.grey[300],
    color: theme.palette.grey[600],
    borderRadius: '50%',
    marginRight: theme.spacing.unit * 2
  }
});

export default itemListStyles;
