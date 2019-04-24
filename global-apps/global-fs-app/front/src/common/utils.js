function getFileTypeByName(name) {
  const nameParsed = name.match(/^.*\.(.+)$/mi);
  const fileFormat = nameParsed && nameParsed[1];

  switch (fileFormat) {
    case 'svg':
    case 'jpg':
    case 'png':
    case 'gif':
      return 'image';
    case 'mp3':
      return 'audio';
    case 'mp4':
      return 'video';
    case 'txt':
      return 'text';
    default:
      return 'unknown';
  }
}

export { getFileTypeByName }
