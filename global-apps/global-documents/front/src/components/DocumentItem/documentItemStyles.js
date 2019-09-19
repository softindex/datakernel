const documentItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    '& > div' : {
      display: 'none'
    },
    '& > button' : {
      display: 'none'
    },
    '&:hover > button': {
      display: 'flex'
    },
    '&:hover > div': {
      display: 'block'
    }
  },
  link: {
    display: 'flex',
    color: theme.palette.secondary.contrastText,
    flexGrow: 1,
    minWidth: theme.spacing(34),
    textDecoration: 'none',
    height: theme.spacing(8),
    alignItems: 'center'
  },
  itemText: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    marginRight: theme.spacing(1)
  }
});

export default documentItemStyles;
