const noteItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 0.5,
    paddingBottom: theme.spacing.unit * 0.5,
    boxShadow: `0px 13px ${theme.spacing.unit}px -${theme.spacing.unit * 2}px rgba(0,0,0,0.5)`
  },
  link: {
    textDecoration: 'none',
    color: theme.palette.secondary.contrastText,
    width: '100%',
    height: theme.spacing.unit * 8
  },
  itemText: {
    marginTop: theme.spacing.unit * 2.5,
    overflow: 'hidden',
    textOverflow: 'ellipsis'
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default noteItemStyles;
