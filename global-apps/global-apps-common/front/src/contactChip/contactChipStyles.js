const contactChipStyles = theme => ({
  chip: {
    margin: theme.spacing(1),
    marginBottom: theme.spacing(2),
    marginTop: 0,
    width: theme.spacing(14),
    overflow: 'hidden'
  },
  chipText: {
    width: 'inherit',
    overflow: 'hidden',
    display: 'inline-block',
    textOverflow: 'ellipsis'
  }
});

export default contactChipStyles;
