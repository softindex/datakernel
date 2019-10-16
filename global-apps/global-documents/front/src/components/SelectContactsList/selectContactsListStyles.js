const selectContactsListStyles = theme => {
  return {
    chatsList: {
      flexGrow: 1,
      overflow: 'auto',
      '&:hover': {
        '&::-webkit-scrollbar-thumb': {
          background: theme.palette.secondary.grey
        }
      },
      height: theme.spacing(37),
      marginTop: theme.spacing(1)
    },
    scroller: {
      scrollbarColor: 'transparent transparent',
      scrollbarWidth: 'thin',
      '&:hover': {
        scrollbarColor: `${theme.palette.secondary.grey} transparent`,
      }
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
