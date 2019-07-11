const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'center',
      alignItems: 'center',
      flexShrink: 1,
      margin: `${theme.spacing.unit}px auto`,
      maxWidth: 850,
    },
    submitIcon: {
      marginRight: theme.spacing.unit
    },
    divider: {
      width: 1,
      height: theme.spacing.unit * 3,
      margin: 4
    },
    input: {
      flexGrow: 1,
      margin: `${theme.spacing.unit}px ${theme.spacing.unit}px ${theme.spacing.unit}px ${theme.spacing.unit * 2}px`
    },
    inputText: {
      color: theme.palette.common.black
    },
    form: {
      padding: `0px ${theme.spacing.unit * 3}px`,
      marginBottom: `${theme.spacing.unit * 2}px`
    }
  }
};

export default messageFormStyles;
