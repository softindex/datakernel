const noteItemStyles = theme => ({
  listItem: {
    width: theme.spacing.unit * 41,
    borderRadius: theme.spacing.unit / 2,
    paddingTop: theme.spacing.unit * 0.375,
    paddingBottom: theme.spacing.unit * 0.375
  },
  link: {
    textDecoration: 'none',
    minWidth: theme.spacing.unit * 31,
    height: theme.spacing.unit * 8
  },
  itemText: {
    marginTop: theme.spacing.unit * 2.5,
    overflow: 'hidden',
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default noteItemStyles;
