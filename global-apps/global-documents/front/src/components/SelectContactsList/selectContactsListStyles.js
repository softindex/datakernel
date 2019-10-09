const selectContactsListStyles = theme => {
  return {
    chatsList: {
      flexGrow: 1,
      '&:hover': {
        overflow: 'auto'
      },
      overflow: 'hidden',
      height: theme.spacing(37),
      marginTop: theme.spacing(1)
    },
    secondaryDividerText: {
      textAlign: 'center',
      margin: `${theme.spacing(2)}px 0px`
    },
    innerUl: {
      padding: 0
    },
    listSubheader: {
      background: theme.palette.primary.contrastText,
      zIndex: 2
    }
  }
};

export default selectContactsListStyles;