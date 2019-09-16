const noteItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    boxShadow: `0px 13px ${theme.spacing(1)}px -${theme.spacing(2)}px rgba(0,0,0,0.5)`
  },
  link: {
    textDecoration: 'none',
    color: theme.palette.secondary.contrastText,
    width: '100%',
    height: theme.spacing(8)
  },
  itemText: {
    marginTop: theme.spacing(2.5),
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
