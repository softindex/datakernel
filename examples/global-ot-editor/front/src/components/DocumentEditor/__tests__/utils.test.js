import {getDifference} from '../utils';

describe('utils.getDifference', () => {
  describe('Nothing to do', () => {
    it('Nothing to do with empty strings', () => {
      const result = getDifference('', '', 0);
      expect(result).toEqual(null);
    });

    it('Nothing to do with not empty strings', () => {
      const result = getDifference('ab', 'ab', 1);
      expect(result).toEqual(null);
    });
  });

  describe('Insert operation', () => {
    it('Inserted one symbol', () => {
      const result = getDifference('', 'a', 1);
      expect(result).toEqual({
        operation: 'insert',
        position: 0,
        content: 'a'
      });
    });

    it('Inserted two symbols', () => {
      const result = getDifference('', 'ab', 2);
      expect(result).toEqual({
        operation: 'insert',
        position: 0,
        content: 'ab'
      });
    });

    it('Inserted one symbol at the end', () => {
      const result = getDifference('aa', 'aaa', 3);
      expect(result).toEqual({
        operation: 'insert',
        position: 2,
        content: 'a'
      });
    });

    it('Inserted one symbol at the beginning', () => {
      const result = getDifference('aa', 'aaa', 1);
      expect(result).toEqual({
        operation: 'insert',
        position: 0,
        content: 'a'
      });
    });

    it('Inserted one symbol in the middle', () => {
      const result = getDifference('aa', 'aaa', 2);
      expect(result).toEqual({
        operation: 'insert',
        position: 1,
        content: 'a'
      });
    });
  });

  describe('Delete operation', () => {
    it('Deleting single symbol', () => {
      const result = getDifference('a', '', 0);
      expect(result).toEqual({
        operation: 'delete',
        position: 0,
        content: 'a'
      });
    });

    it('Deleting two last symbols', () => {
      const result = getDifference('aaa', 'a', 1);
      expect(result).toEqual({
        operation: 'delete',
        position: 1,
        content: 'aa'
      });
    });

    it('Deleting first symbol', () => {
      const result = getDifference('aaa', 'aa', 0);
      expect(result).toEqual({
        operation: 'delete',
        position: 0,
        content: 'a'
      });
    });

    it('Deleting middle one symbol', () => {
      const result = getDifference('aaa', 'aa', 1);
      expect(result).toEqual({
        operation: 'delete',
        position: 1,
        content: 'a'
      });
    });
  });

  describe('Replace operation', () => {
    it('Replace one symbol', () => {
      const result = getDifference('a', 'b', 1);
      expect(result).toEqual({
          operation: 'replace',
          position: 0,
          oldContent: 'a',
          newContent: 'b'
        });
    });

    it('Replace if last symbols equals', () => {
      const result = getDifference('a', 'ba', 2);
      expect(result).toEqual({
          operation: 'replace',
          position: 0,
          oldContent: 'a',
          newContent: 'ba'
        });
    });

    it('Replace if first and last symbols equals', () => {
      const result = getDifference('aaa', 'abaa', 3);
      expect(result).toEqual({
          operation: 'replace',
          position: 1,
          oldContent: 'a',
          newContent: 'ba'
        });
    });
  });
});
