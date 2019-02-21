import { getStringDifference, simplifyOperations } from "../utils";

describe("utils.getStringDifference", () => {
  describe("Nothing to do", () => {
    it("", () => {
      const result = getStringDifference("", "", 0);
      expect(result).toEqual([]);
    });

    it("", () => {
      const result = getStringDifference("ab", "ab", 1);
      expect(result).toEqual([]);
    });
  });

  describe("Insert operation", () => {
    it("Inserted one symbol", () => {
      const result = getStringDifference("", "a", 1);
      expect(result).toEqual([
        {
          type: "insert",
          start: 0,
          length: 1
        }
      ]);
    });

    it("Inserted two symbols", () => {
      const result = getStringDifference("", "ab", 2);
      expect(result).toEqual([
        {
          type: "insert",
          start: 0,
          length: 2
        }
      ]);
    });

    it("Inserted one symbol at the end", () => {
      const result = getStringDifference("aa", "aaa", 3);
      expect(result).toEqual([
        {
          type: "insert",
          start: 2,
          length: 1
        }
      ]);
    });

    it("Inserted one symbol at the beginning", () => {
      const result = getStringDifference("aa", "aaa", 1);
      expect(result).toEqual([
        {
          type: "insert",
          start: 0,
          length: 1
        }
      ]);
    });

    it("Inserted one symbol in the middle", () => {
      const result = getStringDifference("aa", "aaa", 2);
      expect(result).toEqual([
        {
          type: "insert",
          start: 1,
          length: 1
        }
      ]);
    });
  });
  /////////Delete operations
  describe("Delete operation", () => {
    it("Deleting single symbol", () => {
      const result = getStringDifference("a", "", 0);
      expect(result).toEqual([
        {
          type: "delete",
          start: 0,
          length: 1
        }
      ]);
    });

    it("Deleting two last symbols", () => {
      const result = getStringDifference("aaa", "a", 1);
      expect(result).toEqual([
        {
          type: "delete",
          start: 1,
          length: 2
        }
      ]);
    });

    it("Deleting first symbol", () => {
      const result = getStringDifference("aaa", "aa", 0);
      expect(result).toEqual([
        {
          type: "delete",
          start: 0,
          length: 1
        }
      ]);
    });

    it("Deleting middle one symbol", () => {
      const result = getStringDifference("aaa", "aa", 1);
      expect(result).toEqual([
        {
          type: "delete",
          start: 1,
          length: 1
        }
      ]);
    });
  });

  describe("Replace operation", () => {
    it("", () => {
      const result = getStringDifference("a", "b", 1);
      expect(result).toEqual([
        {
          type: "delete",
          start: 0,
          length: 1
        },
        {
          type: "insert",
          start: 0,
          length: 1
        }
      ]);
    });

    it("", () => {
      const result = getStringDifference("a", "ba", 2);
      expect(result).toEqual([
        {
          type: "delete",
          start: 0,
          length: 1
        },
        {
          type: "insert",
          start: 0,
          length: 2
        }
      ]);
    });

    it("", () => {
      const result = getStringDifference("aaa", "abaa", 3);
      expect(result).toEqual([
        {
          type: "delete",
          start: 1,
          length: 1
        },
        {
          type: "insert",
          start: 1,
          length: 2
        }
      ]);
    });
  });
});

it("test", () => {
  const result = getStringDifference("a", "a\n\n", 2);
  expect(result).toEqual([
    {
      type: "insert",
      start: 1,
      length: 2
    }
  ]);
});
