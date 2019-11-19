const searchStyles = theme => ({
  root: {}, // TODO Anton (Упрощение) Если стиль не нужен, то можно его не прописывать и не передавать
  inputDiv: {
    marginLeft: theme.spacing(1),
    marginRight: theme.spacing(3),
    flex: 1
  },
  input: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
  progressWrapper: {
    display: 'flex'
  },
  iconButton: {
    padding: `${theme.spacing(1)}px ${theme.spacing(1)}px`
  }
});

export default searchStyles;
