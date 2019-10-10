const documentItemStyles = theme => ({
  listItem: {
    borderBottom: '1px solid #00000008',
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    '&:hover': {
      background: theme.palette.secondary.lightGrey
    },
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
  selectedItem: {
    background: `${theme.palette.secondary.lightGrey} !important`
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
