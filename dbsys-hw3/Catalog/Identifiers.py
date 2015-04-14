"""
Database internal object identifiers for files, pages, and tuples.

All identifiers implement structural equality.
"""

import struct

class FileId:
  """
  A file identifier class, storing an unsigned short representing a file number.

  We can use a file identifier to retrieve the full path of a file from
  the database catalog. File identifiers implement pack and unpack methods to
  support their storage on disk.

  >>> id1 = FileId(5)
  >>> id2 = FileId.unpack(id1.pack())
  >>> id1 == id2
  True
  """

  binrepr = struct.Struct("H") # represents unsigned short
  size    = binrepr.size

  def __init__(self, fileIndex):
    self.fileIndex = fileIndex

  def __eq__(self, other):
    return self.fileIndex == other.fileIndex

  def __hash__(self):
    return hash(self.fileIndex)

  def pack(self):
    if self.fileIndex != None:
      return FileId.binrepr.pack(self.fileIndex)

  @classmethod
  def unpack(cls, buffer):
    fileIndex = FileId.binrepr.unpack_from(buffer)[0]
    return cls(fileIndex)


class PageId:
  """
  A page identifier class, storing a file identifier and an unsigned short
  representing a page number.

  >>> pId1 = PageId(FileId(5), 100)
  >>> pId2 = PageId.unpack(pId1.pack())
  >>> pId1 == pId2
  True
  """

  binrepr = struct.Struct("H")
  size    = FileId.binrepr.size + binrepr.size

  def __init__(self, fileId, pageIndex):
    self.fileId    = fileId
    self.pageIndex = pageIndex

  def __eq__(self, other):
    return self.fileId == other.fileId and self.pageIndex == other.pageIndex

  def __hash__(self):
    return hash((self.fileId, self.pageIndex))

  def pack(self):
    if self.fileId:
      return self.fileId.pack() + PageId.binrepr.pack(self.pageIndex)

  @classmethod
  def unpack(cls, buffer):
    pageIdSize = PageId.binrepr.size
    fileId     = FileId.unpack(buffer)
    pageIndex  = PageId.binrepr.unpack_from(buffer, offset=FileId.size)[0]
    return cls(fileId, pageIndex)


class TupleId:
  """
  A tuple identifier class, storing a page identifier and an unsigned short
  representing a tuple index.

  The tuple index may have a page-specific interpretation. For example for
  a contiguous page it may denote the tuple's offset within the page, while
  for a slotted page it may denote the slot number.

  The caller must ensure appropriate TupleIds are compared.

  >>> tId1 = TupleId(PageId(FileId(5), 100), 1000)
  >>> tId2 = TupleId.unpack(tId1.pack())
  >>> tId1 == tId2
  True
  """

  binrepr = struct.Struct("H")
  size    = PageId.size + binrepr.size

  def __init__(self, pageId, tupleIndex):
    self.pageId     = pageId
    self.tupleIndex = tupleIndex

  def __eq__(self, other):
    return self.pageId == other.pageId and self.tupleIndex == other.tupleIndex

  def __hash__(self):
    return hash((self.pageId, self.tupleIndex))

  def pack(self):
    if self.pageId:
      return self.pageId.pack() + TupleId.binrepr.pack(self.tupleIndex)

  @classmethod
  def unpack(cls, buffer):
    tupleIdSize = TupleId.binrepr.size
    pageId      = PageId.unpack(buffer)
    tupleIndex  = TupleId.binrepr.unpack_from(buffer, offset=PageId.size)[0]
    return cls(pageId,tupleIndex)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
