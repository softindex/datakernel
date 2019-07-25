const todoItemStyles = theme => ({
  checkbox: {
    margin: `0px ${theme.spacing(1)}px`
  },
  deleteIconButton: {
    margin: `0px ${theme.spacing(1)}px`,
    visibility: 'hidden'
  },
  itemInput: {
    padding: `${theme.spacing(2)}px 0px`,
    '&:before': {
      borderBottom: '1px solid rgba(0, 0, 0, 0.12)',
      borderRadius: theme.spacing(0.5)
    },
    '&:hover > button': {
      visibility: 'visible'
    },
    '&:after': {
      border: '1px solid',
      borderColor: theme.palette.primary.main
    },
    '&:hover:not(.Mui-disabled):before': {
      borderBottom: '2px solid rgba(197, 193, 193)'
    }
  }
});

export default todoItemStyles;
