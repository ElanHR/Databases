import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  defaultPoolSize = 128 * (1 << 20)

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other, **kwargs)

    else:
      self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
      self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)

      self.pool         = io.BytesIO(b'\x00' * self.poolSize)
      self.pageMap      = OrderedDict()
      self.freeList     = list(range(0, self.poolSize, self.pageSize))
      self.freeListLen  = len(self.freeList)

      self.fileMgr      = None

  def fromOther(self, other):
    self.pageSize    = other.pageSize
    self.poolSize    = other.poolSize
    self.pool        = other.pool
    self.pageMap     = other.pageMap
    self.freeList    = other.freeList
    self.freeListLen = other.freeListLen
    self.fileMgr     = other.fileMgr

  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr


  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return self.freeListLen

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in self.pageMap
  
  # Gets a page from the buffer pool if present, otherwise reads it from a heap file.
  # This method returns both the page, as well as a boolean to indicate whether
  # there was a cache hit.
  def getPageWithHit(self, pageId, pinned=False):
    if self.fileMgr:
      if self.hasPage(pageId):
        return (self.getCachedPage(pageId, pinned)[1], True)

      else:
        # Fetch the page from the file system, adding it to the buffer pool
        if not self.freeList:
          self.evictPage()

        self.freeListLen -= 1
        offset     = self.freeList.pop(0)
        pageBuffer = self.pool.getbuffer()[offset:offset+self.pageSize]
        page       = self.fileMgr.readPage(pageId, pageBuffer)
        
        self.pageMap[pageId] = (offset, page, 1 if pinned else 0)
        self.pageMap.move_to_end(pageId)
        return (page, False)
    
    else:
      raise ValueError("Uninitalized buffer pool, no file manager found")

  # Wrapper for getPageWithHit, returning only the page.
  def getPage(self, pageId, pinned=False):
    return self.getPageWithHit(pageId, pinned)[0]

  # Returns a triple of offset, page object, and pin count
  # for pages present in the buffer pool.
  def getCachedPage(self, pageId, pinned=False):
    if self.hasPage(pageId):
      if pinned:
        self.incrementPinCount(pageId, 1)
      return self.pageMap[pageId]
    else:
      return (None, None, None)

  # Pins a page.
  def pinPage(self, pageId):
    if self.hasPage(pageId):
      self.incrementPinCount(pageId, 1)

  # Unpins a page.
  def unpinPage(self, pageId):
    if self.hasPage(pageId):
      self.incrementPinCount(pageId, -1)

  # Returns the pin count for a page.
  def pagePinCount(self, pageId):
    if self.hasPage(pageId):
      return self.pageMap[pageId][2]

  # Update the pin counter for a cached page.
  def incrementPinCount(self, pageId, delta):
    (offset, page, pinCount) = self.pageMap[pageId]
    self.pageMap[pageId] = (offset, page, pinCount+delta)

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if self.hasPage(pageId):
      (offset, _, pinCount) = self.pageMap[pageId]
      if pinCount == 0:
        self.freeList.append(offset)
        self.freeListLen += 1
        del self.pageMap[pageId]

  # Removes a page from the page map, returning it to the free 
  # page list. This method also flushes the page to disk.
  def flushPage(self, pageId):
    if self.fileMgr:
      (offset, page, pinCount) = self.getCachedPage(pageId)
      if all(map(lambda x: x is not None, [offset, page, pinCount])):
        if pinCount == 0:
          self.freeList.append(offset)
          self.freeListLen += 1
          del self.pageMap[pageId]

        if page.isDirty():
          self.fileMgr.writePage(page)
    else:
      raise ValueError("Uninitalized buffer pool, no file manager found")

  # Evict using LRU policy, considering only unpinned pages.
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    if self.pageMap:
      # Find an unpinned page to evict.
      pageToEvict = None
      for (pageId, (_, _, pinCount)) in self.pageMap.items():
        if pinCount == 0:
          pageToEvict = pageId
          break

      if pageToEvict:
        self.flushPage(pageToEvict)

      else:
        raise ValueError("Could not find a page to evict in the buffer pool")

  def clear(self):
    for (pageId, (offset, page, _)) in self.pageMap.items():
      if page.isDirty():
        self.flushPage(pageId)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
