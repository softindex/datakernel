const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'center',
      alignItems: 'center',
      flexShrink: 1,
      margin: `${theme.spacing(1)}px auto`,
      maxWidth:  theme.spacing(1) * 106
    },
    submitIcon: {
      marginRight: theme.spacing(1)
    },
    divider: {
      width: 1,
      height: theme.spacing(3),
      margin: theme.spacing(0.5)
    },
    input: {
      flexGrow: 1,
      margin: `${theme.spacing(1)}px ${theme.spacing(1)}px ${theme.spacing(1)}px ${theme.spacing(2)}px`
    },
    inputText: {
      color: theme.palette.common.black
    },
    form: {
      padding: `0px ${theme.spacing(3)}px`,
      marginBottom: theme.spacing(2)
    }
  }
};

export default messageFormStyles;
