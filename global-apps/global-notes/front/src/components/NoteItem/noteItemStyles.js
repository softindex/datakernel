const noteItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    '& > div': {
      display: 'none'
    },
    '&:hover > div': {
      display: 'block'
    },
    boxShadow: `0px 13px ${theme.spacing(1)}px -${theme.spacing(2)}px rgba(0,0,0,0.5)`
  },
  link: {
    display: 'flex',
    flexGrow: 1,
    minWidth: theme.spacing(34),
    textDecoration: 'none',
    height: theme.spacing(8),
    alignItems: 'center',
    color: theme.palette.secondary.contrastText
  },
  itemText: {
    overflow: 'hidden',
    textOverflow: 'ellipsis'
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
});

export default noteItemStyles;
