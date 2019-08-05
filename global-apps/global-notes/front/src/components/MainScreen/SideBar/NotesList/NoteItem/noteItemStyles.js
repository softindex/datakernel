const noteItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 0.5,
    paddingBottom: theme.spacing.unit * 0.5,
    '& > div' : {
      display: 'none'
    },
    '&:hover > div': {
      display: 'block'
    }
  },
  link: {
    display: 'flex',
    flexGrow: 1,
    minWidth: theme.spacing.unit * 33.75,
    textDecoration: 'none',
    height: theme.spacing.unit * 8,
    alignItems: 'center'
  },
  itemText: {
    overflow: 'hidden'
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
});

export default noteItemStyles;
