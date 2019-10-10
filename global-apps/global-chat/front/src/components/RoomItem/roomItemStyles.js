const roomItemStyles = theme => ({
  listItem: {
    borderBottom: '1px solid #00000008',
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
    '&:hover': {
      background: theme.palette.secondary.lightGrey
    },
    '& > div' : {
      visibility: 'hidden'
    },
    '&:hover > button': {
      display: 'flex'
    },
    '&:hover > div': {
      visibility: 'visible'
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
    alignItems: 'center'
  },
  itemText: {
    overflow: 'hidden',
    color: theme.palette.secondary.contrastText,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap'
  },
  deleteIcon: {
    display: 'none'
  }
});

export default roomItemStyles;