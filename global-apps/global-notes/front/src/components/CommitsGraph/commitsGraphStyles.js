const commitsGraphStyles = theme => ({
  root: {
    height: '100vh',
    overflow: 'auto',
    background: theme.palette.common.white,
    display: 'flex',
    justifyContent: 'center',
    padding: theme.spacing.unit * 2,
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.secondary.grey
    }
  }
});

export default commitsGraphStyles;
