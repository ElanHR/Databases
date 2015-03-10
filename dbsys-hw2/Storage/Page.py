from io import BytesIO
import copy, math, struct

from Catalog.Identifiers import TupleId

class PageHeader:
  """
  A base class for page headers, storing bookkeeping information on a page.

  Page headers implement structural equality over their component fields.
  
  This includes the page's flags (e.g., whether the page is dirty), as well as
  the tuple size for a page, the free space offset within a page and the
  page's capacity.

  This simple page header supports only fixed-size tuples, and a write-once
  implementation of pages by using only a free space offset. That is, the
  free space offset monotonically increases as tuples are inserted into the
  page. Reclaiming space following tuple deletion requires vacuuming (i.e.,
  page reorganization and defragmentation).

  The header size is provided by an explicit method in the base class, and this
  method should be overriden by subclasses to account for the size of any
  additional fields. The exact size of a PageHeader can always be retrieved by
  the 'PageHeader.size' class attribute.

  PageHeaders implement pack and unpack methods to support their storage as
  in-memory buffers and on disk.

  Page headers require the page's backing buffer as a constructor argument.
  This buffer must support Python's buffer protocol, for example as provided
  by a 'memoryview' object. Furthermore, the buffer must be writeable.

  On construction, the page header stores a packed representation of itself
  at the beginning of the page. A page lazily maintains its page header in
  its backing buffer, working primarily with the in-memory representation
  instead. That is, while tuples are inserted and deleted in the page, only
  the Python PageHeader object is directly maintained. It is only when the page
  itself is packed that the page header in the page's buffer is refreshed.

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = PageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = PageHeader.unpack(buffer.getbuffer())
  >>> ph == ph2
  True

  >>> buffer2 = io.BytesIO(bytes(2048))
  >>> ph3     = PageHeader(buffer=buffer2.getbuffer(), tupleSize=16)
  >>> ph == ph3
  False

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the header boundary
  >>> ph.nextFreeTuple() == ph.headerSize()
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0,tuplesToTest)]
  [24, 40, 56, 72, 88, 104, 120, 136, 152, 168]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [184, 200, ..., 4072]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """

  binrepr   = struct.Struct("cHHH") # char + 3 unsigned shorts
  size      = binrepr.size
  
  # Flag bitmasks
  dirtyMask     = 0b1
  directoryPage = 0b11

  def __init__(self, **kwargs):
    other = kwargs.get("other", None) 
    if other:
      self.fromOther(other)

    else:
      # Constructors:
      # i. buffer, tupleSize, [flags, fso, pagecap]
      buffer = kwargs.get("buffer", None)

      if not buffer:
        raise ValueError("No buffer specified for a page header.")
      
      self.flags           = kwargs.get("flags", b'\x00')
      self.tupleSize       = kwargs.get("tupleSize", None)
      self.pageCapacity    = kwargs.get("pageCapacity", len(buffer))

      if not self.tupleSize:
        raise ValueError("No tuple size specified in a page header.")

      self.postHeaderInitialize(**kwargs)

  def __eq__(self, other):
    return (    self.tupleSize == other.tupleSize
            and self.flags == other.flags
            and self.freeSpaceOffset == other.freeSpaceOffset
            and self.pageCapacity == other.pageCapacity )

  def postHeaderInitialize(self, **kwargs):
    fresh  = kwargs.get("flags", None) is None
    buffer = kwargs.get("buffer", None)
    
    self.freeSpaceOffset = kwargs.get("freeSpaceOffset", self.headerSize())
    if fresh and buffer:
      # Push the page header into the buffer.
      # Any subclasses of the page header must do the same for its metadata.
      buffer[0:PageHeader.size] = PageHeader.pack(self)

  def fromOther(self, other):
    if isinstance(other, PageHeader): 
        self.tupleSize       = other.tupleSize
        self.flags           = other.flags
        self.freeSpaceOffset = other.freeSpaceOffset
        self.pageCapacity    = other.pageCapacity

  def headerSize(self):
    return PageHeader.size

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  # Tuple count for the header.
  def numTuples(self):
    return int(self.usedSpace() / self.tupleSize)

  # Tuple index for a given offset
  def tupleIndex(self, offset):
    return math.floor((offset - self.dataOffset()) / self.tupleSize)

  # Check that an offset is in a writable data area in the page
  def validatePageOffset(self, offset):
    if self.dataOffset() <= offset and offset < self.pageCapacity:
      return offset

  # Check that an offset is within the written data area
  def validateDataOffset(self, offset):
    if self.dataOffset() <= offset and offset <= self.freeSpaceOffset:
      return offset

  def validTuple(self, tupleData):
    return len(tupleData) == self.tupleSize

  # Returns the offset in the page where the data begins
  def dataOffset(self):
    return self.headerSize()

  # Check that the tuple index is within the current page and return its offset
  def tupleIndexOffset(self, tupleIndex):
    if self.tupleSize:
      return self.validatePageOffset(self.dataOffset() + (self.tupleSize * tupleIndex))

  # Check that a given tuple is within the current page and return its offset
  def tupleOffset(self, tupleId):
    if tupleId:
      return self.tupleIndexOffset(tupleId.tupleIndex)

  # Return the range of offsets within the page occupied by a tuple
  def tupleRange(self, tupleId):
    start = self.tupleOffset(tupleId)
    end   = start + self.tupleSize if start else None
    if end:
      return (self.validateDataOffset(start), self.validateDataOffset(end))
    else:
      return (None, None)

  # Return the range of offsets that the tuple may occupy
  def pageRange(self, tupleId):
    start = self.tupleOffset(tupleId)
    end   = start + self.tupleSize if start else None
    if end:
      return (start, self.validatePageOffset(end))
    else:
      return (None, None)

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.freeSpaceOffset

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return self.freeSpaceOffset - self.dataOffset()

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return self.freeSpaceOffset + self.tupleSize <= self.pageCapacity

  # Returns the page offset of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.hasFreeTuple():
      self.freeSpaceOffset += self.tupleSize
      return self.freeSpaceOffset - self.tupleSize
    else:
      return None

  # Returns a triple of (tupleIndex, start, end) for the next free tuple.
  def nextTupleRange(self):
    if self.hasFreeTuple():
      start = self.nextFreeTuple()
      end   = start + self.tupleSize if start else None
      index = self.tupleIndex(start) if start else None
      return (index, start, end)
    else:
      return (None, None, None)

  # Marks the tuple as being used if it is not already so.
  # In a contiguous tuple, the caller must ensure all data up to the tuple id is valid.
  def useTupleIndex(self, tupleIndex):
    offset = self.tupleIndexOffset(tupleIndex)
    if offset + self.tupleSize >= self.freeSpaceOffset:
      self.freeSpaceOffset = offset + self.tupleSize

  def useTuple(self, tupleId):
    self.useTupleIndex(tupleId.tupleIndex)

  # Marks the tuple as being free. 
  # In a contiguous tuple, all tuples after the given tuple id become free.
  def resetTupleIndex(self, tupleIndex):
    self.freeSpaceOffset = self.tupleIndexOffset(tupleIndex)

  def resetTuple(self, tupleId):
    self.resetTupleIndex(tupleId.tupleIndex)

  def pack(self):
    return PageHeader.binrepr.pack(
              self.flags, self.tupleSize,
              self.freeSpaceOffset, self.pageCapacity)

  @classmethod
  def unpack(cls, buffer):
    values = PageHeader.binrepr.unpack_from(buffer)
    if len(values) == 4:
      return cls(buffer=buffer, flags=values[0], tupleSize=values[1],
                 freeSpaceOffset=values[2], pageCapacity=values[3])


class Page(BytesIO):
  """
  A page class, representing a unit of storage for database tuples.

  A page includes a page identifier, and a page header containing metadata
  about the state of the page (e.g., its free space offset).

  Our page class inherits from an io.BytesIO, providing it an implementation
  of a in-memory binary stream. 

  The page constructor requires a byte buffer in which we can store tuples.
  The user has the responsibility for constructing a suitable buffer, for
  example with Python's 'bytes()' builtin.

  The page also provides several methods to retrieve and modify its contents
  based on a tuple identifier, and where relevant, tuple data represented as
  an immutable sequence of bytes.

  The page's pack and unpack methods can be used to obtain a byte sequence
  capturing both the page header and tuple data information for storage on disk.
  The page's pack method is responsible for refreshing the in-buffer representation
  of the page header prior to return the entire page as a byte sequence.
  Currently this byte-oriented representation does not capture the page identifier.
  This is left to the file structure to inject into the page when constructing
  this Python object.

  This class imposes no restriction on the page size.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = Page(pageId=pId, buffer=bytes(4096), schema=schema)

  # Test page packing and unpacking
  >>> len(p.pack())
  4096
  >>> p2 = Page.unpack(pId, p.pack())
  >>> p.pageId == p2.pageId
  True
  >>> p.header == p2.header
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()
  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)
  
  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's data segment has been compacted after the remove.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = PageHeader

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      buffer = kwargs.get("buffer", None)
      if buffer:
        BytesIO.__init__(self, buffer)
        self.pageId = kwargs.get("pageId", None)
        header      = kwargs.get("header", None)

        if self.pageId and header:
          self.header = header
        elif self.pageId:
          self.header = self.initializeHeader(**kwargs)
        else:
          raise ValueError("No page identifier provided to page constructor.")
      else:
        raise ValueError("No backing buffer provided to page constructor.")      

  def fromOther(self, other):
    BytesIO.__init__(self, other.getvalue())
    self.pageId = copy.deepcopy(other.pageId)
    self.header = copy.deepcopy(other.header)

  # Header constructor. This can be overridden by subclasses.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return PageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a page.")

  # Tuple iterator
  def __iter__(self):
    return PageTupleIterator(self)

  # Dirty bit accessors
  def isDirty(self):
    return self.header.isDirty()

  def setDirty(self, dirty):
    self.header.setDirty(dirty)

  # Tuple accessor methods
  def getTuple(self, tupleId):
    if self.header and tupleId:
      (start, end) = self.header.tupleRange(tupleId)
      if start and end:
        return self.getbuffer()[start:end]

  def putTuple(self, tupleId, tupleData):
    if self.header and tupleId and tupleData and self.header.validTuple(tupleData):
      (start, end) = self.header.tupleRange(tupleId)
      if start and end:
        self.setDirty(True)
        self.getbuffer()[start:end] = tupleData

  def insertTuple(self, tupleData):
    if self.header and tupleData and self.header.validTuple(tupleData):
      (tupleIndex, start, end) = self.header.nextTupleRange()
      if start and end:
        self.setDirty(True)
        self.getbuffer()[start:end] = tupleData
        return TupleId(self.pageId, tupleIndex)

  def clearTuple(self, tupleId):
    if self.header and tupleId:
      (start, end) = self.header.tupleRange(tupleId)
      if start and end:
        self.setDirty(True)
        self.getbuffer()[start:end] = b'\x00' * self.header.tupleSize

  def deleteTuple(self, tupleId):
    if self.header and tupleId:
      (start, end) = self.header.tupleRange(tupleId)
      if start and end:
        self.setDirty(True)
        shiftLen = self.header.freeSpaceOffset - end
        self.getbuffer()[start:start+shiftLen] = self.getbuffer()[end:end+shiftLen]
        resetTupleIndex = self.header.tupleIndex(self.header.freeSpaceOffset - self.header.tupleSize)
        self.header.resetTuple(TupleId(self.pageId, resetTupleIndex))

  def clear(self):
    if self.header:
      start = self.header.dataOffset()
      end   = self.header.pageCapacity
      if start and end:
        self.setDirty(True)
        self.getbuffer()[start:end] = b'\x00' * (end-start)

  def pack(self):
    if self.header:
      self.getbuffer()[0:self.header.headerSize()] = self.header.pack()
      return self.getvalue()

  @classmethod
  def unpack(cls, pageId, buffer):
    header = cls.headerClass.unpack(buffer)
    return cls(pageId=pageId, buffer=buffer, header=header)

class PageTupleIterator:
  """
  Explicit tuple iterator class, for ranging over the tuples in a page.
  This allows multiple callers to simultaneously iterate over the same page.
  """
  def __init__(self, page):
    self.page = page
    self.iterTupleIdx = 0

  def __iter__(self):
    return self

  def __next__(self):
    t = self.page.getTuple(TupleId(self.page.pageId, self.iterTupleIdx))
    if t:
      self.iterTupleIdx += 1
      return t
    else:
      raise StopIteration

if __name__ == "__main__":
    import doctest
    doctest.testmod()
