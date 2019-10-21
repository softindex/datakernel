const itemListStyles = theme => ({
  root: {
    height: '100%',
    maxWidth: '100%',
    display: 'flex',
    flexDirection: 'column',
    padding: theme.spacing(2),
    flexGrow: 1,
    overflowX: 'hidden'
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
    margin: `${theme.spacing(2)}px 0`,
    flexDirection: 'column'
  },
  emptyIndicator: {
    padding: theme.spacing(2),
    width: theme.spacing(8),
    height: theme.spacing(8),
    backgroundColor: theme.palette.grey[300],
    color: theme.palette.grey[600],
    borderRadius: '50%',
    marginRight: theme.spacing(2)
  }
});

export default itemListStyles;
