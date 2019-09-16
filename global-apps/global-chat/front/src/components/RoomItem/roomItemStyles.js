const roomItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(0.5),
    paddingBottom: theme.spacing(0.5),
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