import functools, math, struct, sys
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page, PageTupleIterator

class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This stores a slot array
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

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

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

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
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True

  >>> ph.freeSpace() < ph.tupleSize
  True
  """

  # # Slots are two unsigned shorts: slot offset and slot data length
  # slotRepr    = Struct("HH")
  # slotSize    = slotRepr.size

  prefixFmt   = "H"
  prefixRepr  = struct.Struct(prefixFmt)

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other, **kwargs)

    else:
      buffer = kwargs.get("buffer", None)
      parent = kwargs.get("parent", None)
      if buffer:
        if parent:
          super().__init__(other=parent)
        else:
          super().__init__(**kwargs)

        self.numSlots = kwargs.get("numSlots", self.maxTuples())
        self.slots    = self.initializeSlots(buffer)
        self.binrepr  = Struct(SlottedPageHeader.prefixFmt+str(self.slotBufferSize())+"s")
        self.reprSize = PageHeader.size + self.binrepr.size

        # Call postHeaderInitialize now that we've initialized our local attributes
        self.postHeaderInitialize(**kwargs)

      else:
        raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def __eq__(self, other):
    return super().__eq__(other) and (
            self.numSlots == other.numSlots
            and self.slots == other.slots )

  def postHeaderInitialize(self, **kwargs):
    # Check local attributes have been initialized
    if hasattr(self, "reprSize"):
      fresh  = kwargs.get("unpacked", None) is None
      buffer = kwargs.get("buffer", None)
      parent = kwargs.get("parent", None)

      if parent is None:
        super().postHeaderInitialize(**kwargs)

      # Push the subclass metadata into the buffer.
      # In this case, we also push the parent page header due
      # to the updated free space offset.
      if fresh and buffer:
        start = PageHeader.size
        end   = start + SlottedPageHeader.prefixRepr.size
        buffer[start:end] = SlottedPageHeader.prefixRepr.pack(self.numSlots)
        self.slots[:] = b'\x00' * self.slotBufferSize()
      else:
        self.slots[:] = kwargs.get("slots", b'\x00' * self.slotBufferSize())

  def fromOther(self, other):
    super().fromOther(other)
    if isinstance(other, SlottedPageHeader):
      self.numSlots = other.numSlots
      self.slots    = other.slots
      self.binrepr  = other.binrepr
      self.reprSize = other.reprSize

  # Parent method overrides
  def headerSize(self):
    return self.reprSize

  def numTuples(self):
    return len(self.usedSlots())

  # Returns the maximum number of tuples that can be held in this page.
  def maxTuples(self):
    headerSize = PageHeader.size + SlottedPageHeader.prefixRepr.size
    headerPerTuple = 0.125
    return math.floor((self.pageCapacity - headerSize) / (self.tupleSize + headerPerTuple))

  # Returns the length of the bitvector for slots, in bytes.
  def slotBufferSize(self):
    if self.numSlots:
      sz = self.numSlots >> 3
      if self.numSlots % 8 == 0:
        return sz
      else:
        return sz + 1

  # Initializes the bitvector object for slots.
  def initializeSlots(self, buffer):
    if self.numSlots:
      start = PageHeader.size + SlottedPageHeader.prefixRepr.size
      end   = start + self.slotBufferSize()
      return memoryview(buffer[start:end])
    else:
      raise ValueError("Unable to initialize slots, do not know number of slots")

  # Slotted page specific methods

  # Returns the byte offset of the given slot in the bitvector.
  def slotBufferByteOffset(self, slotIndex):
    return slotIndex >> 3

  # Returns the byte offset and bit offset of the given slot in the bitvector.
  def slotBufferOffset(self, slotIndex):
    return (slotIndex >> 3, 7 - (slotIndex % 8))

  def hasSlot(self, slotIndex):
    offset = self.slotBufferByteOffset(slotIndex)
    return 0 <= offset and offset < self.slots.nbytes

  def getSlot(self, slotIndex):
    if self.hasSlot(slotIndex):
      (byteIdx, bitIdx) = self.slotBufferOffset(slotIndex)
      return bool(self.slots[byteIdx] & (0b1 << bitIdx))
    else:
      raise ValueError("Invalid get slot index")

  def setSlot(self, slotIndex, used):
    if self.hasSlot(slotIndex):
      (byteIdx, bitIdx) = self.slotBufferOffset(slotIndex)
      if used:
        self.slots[byteIdx] = self.slots[byteIdx] | (0b1 << bitIdx)
      else:
        self.slots[byteIdx] = self.slots[byteIdx] & ~(0b1 << bitIdx)
    else:
      raise ValueError("Invalid set slot index or slot value")

  # Marks a slot as free.
  def resetSlot(self, slotIndex):
    self.setSlot(slotIndex, False)

  # Returns the slot indexes for all of the unused slots.
  def freeSlots(self):
    freeIndexes = []
    for i in range(self.slots.nbytes-1):
      for j in range(8):
        if not ( self.slots[i] & (0b1 << (7 - j)) ):
          freeIndexes.append((i << 3) + j)

    # Special handling of the final byte to only consider up to numSlots.
    i = self.slots.nbytes-1
    j = 0
    while (i << 3) + j < self.numSlots:
      if not ( self.slots[i] & (0b1 << (7 - j)) ):
        freeIndexes.append((i << 3) + j)
      j += 1

    return freeIndexes

  # Returns the slot indexes for all used slots.
  def usedSlots(self):
    usedIndexes = []
    for i in range(self.slots.nbytes-1):
      for j in range(8):
        if self.slots[i] & (0b1 << (7 - j)):
          usedIndexes.append((i << 3) + j)

    # Special handling of the final byte to only consider up to numSlots.
    i = self.slots.nbytes-1
    j = 0
    while (i << 3) + j < self.numSlots:
      if self.slots[i] & (0b1 << (7 - j)):
        usedIndexes.append((i << 3) + j)
      j += 1

    return usedIndexes

  # Converts an absolute page offset into a slot index.
  def tupleIndex(self, offset):
    tupleIdx = None
    for i in self.usedSlots():
      start = self.slotTupleOffset(i)
      end   = start + self.tupleSize
      if start <= offset and offset < end:
        tupleIdx = i
        break
    return tupleIdx

  # Returns the offset within the page for the tuple corresponding to a slot.
  def slotOffset(self, slotIndex):
    if slotIndex is not None and self.tupleSize:
      return self.validatePageOffset(self.dataOffset() + (self.tupleSize * slotIndex))

  # Slotted pages interpret tuple indexes as the slot index.
  def tupleOffset(self, tupleId):
    if tupleId:
      return self.slotOffset(tupleId.tupleIndex)

  def tupleRange(self, tupleId):
    if tupleId and self.tupleSize and self.getSlot(tupleId.tupleIndex):
      start = self.tupleOffset(tupleId)
      end   = start + self.tupleSize
      return (self.validateDataOffset(start), self.validateDataOffset(end))
    else:
      return (None, None)

  def pageRange(self, tupleId):
    if tupleId and self.tupleSize and self.getSlot(tupleId.tupleIndex):
      start = self.tupleOffset(tupleId)
      end   = start + self.tupleSize
      return (self.validatePageOffset(start), self.validatePageOffset(end))
    else:
      return (None, None)

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - (self.dataOffset() + self.usedSpace())

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    usage = sum([self.tupleSize for i in self.usedSlots()])
    return usage if isinstance(usage, int) else 0

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    fullSlots = bytearray(b'\xff' * self.slots.nbytes)
    if self.numSlots % 8 != 0:
      b = 0
      for i in range(self.numSlots % 8):
        b = b | (0b1 << (7 - i))
      fullSlots[self.slots.nbytes-1] = b

    return self.slots != fullSlots

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    index = None
    for i in range(self.slots.nbytes-1):
      # Compute the first free slot by:
      # i. xor'ing with all bits set to determine the free bit mask
      # ii. differencing against the bit length (i.e., the # of bits to represent the free mask)
      slotInByte = 8 - ( (self.slots[i] ^ 0xff).bit_length() )
      if slotInByte < 8:
        index = (i << 3) + slotInByte
        self.useTupleIndex(index)
        break

    # Special handling of the final byte to only consider up to numSlots.
    if index is None:
      i = self.slots.nbytes-1
      j = 0
      while index is None and (i << 3) + j < self.numSlots:
        if not ( self.slots[i] & (0b1 << (7 - j)) ):
          index = (i << 3) + j
          self.useTupleIndex(index)
        j += 1

    return index

  def nextTupleRange(self):
    tupleIndex = self.nextFreeTuple()
    start      = self.slotOffset(tupleIndex) if tupleIndex is not None else None
    end        = start + self.tupleSize      if tupleIndex is not None else None
    return (tupleIndex, start, end)

  # Marks the tuple as being used if it is not already so.
  # In a slotted page, we set the given slot to refer to its corresponding
  # tuple data segment, and then ensure the parent's freeSpaceOffset covers this segment.
  def useTupleIndex(self, tupleIndex):
    self.setSlot(tupleIndex, True)
    super().useTupleIndex(tupleIndex)

  # Marks the tuple as being free.
  # In a slotted page, we reset the given slot. Note we do not update the
  # parent's freeSpaceOffset since the tuple's validity is overriden by the slot.
  def resetTupleIndex(self, tupleIndex):
    self.resetSlot(tupleIndex)

  def pack(self):
    if self.numSlots and self.slots:
      return super().pack() + self.binrepr.pack(self.numSlots, self.slots.tobytes())

  @classmethod
  def binrepr(cls, buffer):
    lenStruct    = Struct("H")
    numSlots     = lenStruct.unpack_from(buffer, offset=PageHeader.size)[0]
    slotArrayLen = numSlots >> 3
    if numSlots % 8 != 0:
      slotArrayLen += 1
    if numSlots > 0:
      return Struct(SlottedPageHeader.prefixFmt+str(slotArrayLen)+"s")
    else:
      raise ValueError("Invalid number of slots in slotted page header")

  @classmethod
  def unpack(cls, buffer):
    parent = PageHeader.unpack(buffer)
    brepr  = cls.binrepr(buffer)
    (numSlots, slotBuffer) = brepr.unpack_from(buffer, offset=PageHeader.size)
    return cls(parent=parent, buffer=buffer, \
               numSlots=numSlots, slots=slotBuffer, unpacked=True)



class SlottedPage(Page):
  """
  A slotted page implementation, inheriting from the Page class.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  This page class does not need to override any of the methods in its
  base class. Rather all of the logic is handled in the slotted page
  header and its intepretation of a tuple identifier's index field as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

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

  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  def __init__(self, pageId, buffer, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other, **kwargs)

    else:
      header = kwargs.get("header", None)
      if header:
        super().__init__(pageId=pageId, buffer=buffer, header=header)
      else:
        super().__init__(pageId=pageId, buffer=buffer, **kwargs)

  def fromOther(self, other):
    super().__init__(other=other)

  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator
  def __iter__(self):
    return SlottedPageTupleIterator(self)

  # Override contiguous page's deleteTuple to prevent it shifting data.
  def deleteTuple(self, tupleId):
    if self.header and tupleId:
      self.clearTuple(tupleId)
      self.header.resetTuple(tupleId)


class SlottedPageTupleIterator(PageTupleIterator):
  """
  Iteration over the tuples in a slotted page.
  """
  def __init__(self, page):
    if not isinstance(page, SlottedPage):
      raise ValueError("Invalid slotted page instance for a slotted page iterator")
    super().__init__(page)

  def __iter__(self):
    return self

  # Tuple iterator
  def __next__(self):
    (start, end) = (None, None)
    while (start is None or end is None) and self.iterTupleIdx < self.page.header.maxTuples():
      tId = TupleId(self.page.pageId, self.iterTupleIdx)
      (start, end) = self.page.header.tupleRange(tId)
      self.iterTupleIdx += 1

    if start and end:
      t = self.page.getTuple(tId)
      if t:
        return t

    raise StopIteration

if __name__ == "__main__":
    import doctest
    doctest.testmod()
