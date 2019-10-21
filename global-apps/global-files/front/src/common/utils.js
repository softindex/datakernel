export function getFileTypeByName(name) {
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

export const PREFIX_TO_IGNORE = '.~#!HIDDEN!#~.';

export function ascStringComparator(a, b) {
  return a.name.localeCompare(b.name);
}

export function escapeSpecialChars(unsafe) {
  return unsafe
    .replace(/\*/g, '\\*')
    .replace(/\?/g, '\\?')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
}

