const noteItemStyles = theme => ({
  listItem: {
    borderBottom: '1px solid #00000012',
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    '&:hover': {
      background: theme.palette.secondary.lightGrey
    },
    '& > div': {
      display: 'none'
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
