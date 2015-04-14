import json, io, os, os.path, pickle

from Catalog.Schema             import DBSchema
from Catalog.Identifiers        import FileId
from Storage.File               import StorageFile
from Storage.Index.IndexManager import IndexManager

class FileManager:
  """
  A file manager, maintaining the storage files for the database relations.

  The file manager is implemented as two dictionaries, one mapping the
  relation name to a file identifier, and the second mapping a file
  identifier to the storage file object.

  >>> import Storage.BufferPool
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = Storage.BufferPool.BufferPool()
  >>> fm = FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Test addition and removal of relations
  >>> fm.createRelation(schema.name, schema)
  >>> list(fm.relations())
  ['employee']

  >>> (fId, rFile) = fm.relationFile(schema.name)

  >>> fm.removeRelation(schema.name, detach=True)
  >>> list(fm.relations())
  []

  >>> fm.addRelation(schema.name, fId, rFile)
  >>> list(fm.relations())
  ['employee']

  # Test FileManager construction on existing directory
  >>> fm = FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)
  >>> list(fm.relations())
  ['employee']
  """

  defaultDataDir     = "data/"
  defaultFileClass   = StorageFile

  checkpointEncoding = "latin1"
  checkpointFile     = "db.fm"

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      self.bufferPool      = kwargs.get("bufferPool", None)
      self.dataDir         = kwargs.get("dataDir", FileManager.defaultDataDir)
      self.indexDir        = kwargs.get("indexDir", os.path.join(self.dataDir, "index"))
      self.defaultPageSize = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)

      if self.bufferPool is None:
        raise ValueError("No buffer pool found when initializing a file manager")

      checkpointFound = os.path.exists(os.path.join(self.dataDir, FileManager.checkpointFile))
      restoring       = "restore" in kwargs

      if not os.path.exists(self.dataDir):
        os.makedirs(self.dataDir)

      if restoring or not checkpointFound:
        self.fileClass     = kwargs.get("fileClass", FileManager.defaultFileClass)
        self.fileCounter   = kwargs.get("fileCounter", 0)
        self.relationFiles = kwargs.get("relationFiles", {})
        self.fileMap       = kwargs.get("fileMap", {})
        self.indexManager  = kwargs.get("indexManager", IndexManager(indexDir=self.indexDir))

        if restoring:
          self.relationFiles = dict([(i[0], FileId(i[1])) for i in kwargs["restore"][0]])
          for i in kwargs["restore"][1]:
            fId   = FileId(i[0])
            fPath = i[1]
            self.fileMap[fId] = \
              self.fileClass(bufferPool=self.bufferPool, fileId=fId, filePath=fPath, mode="update")

      else:
        self.restore()

  def fromOther(self, other):
    self.bufferPool      = other.bufferPool
    self.dataDir         = other.dataDir
    self.defaultPageSize = other.defaultPageSize
    self.fileClass       = other.fileClass
    self.fileCounter     = other.fileCounter
    self.relationFiles   = other.relationFiles
    self.fileMap         = other.fileMap
    self.indexDir        = other.indexDir
    self.indexManager    = other.indexManager

  # Closes and flushes all storage files in the file manager.
  # This includes flushing all pages held in the buffer pool.
  def close(self):
    if self.bufferPool:
      self.bufferPool.clear()

    if self.fileMap:
      for storageFile in self.fileMap.values():
        storageFile.close()

    if self.indexManager:
      self.indexManager.close()

    self.checkpoint()

  # Save the file manager internals to the data directory.
  # The index manager is responsible for checkpointing itself.
  def checkpoint(self):
    fmPath = os.path.join(self.dataDir, FileManager.checkpointFile)
    with open(fmPath, 'w', encoding=FileManager.checkpointEncoding) as f:
      f.write(self.pack())

  # Load relations from an existing data directory.
  def restore(self):
    fmPath = os.path.join(self.dataDir, FileManager.checkpointFile)
    with open(fmPath, 'r', encoding=FileManager.checkpointEncoding) as f:
      other = FileManager.unpack(self.bufferPool, f.read())
      self.fromOther(other)

  # Return the relation ids present in the file manager.
  def relations(self):
    return self.relationFiles.keys()

  def hasRelation(self, relId):
    return relId in self.relationFiles

  def createRelation(self, relId, schema):
    if relId not in self.relationFiles:
      fId = FileId(self.fileCounter)
      path = os.path.join(self.dataDir, str(self.fileCounter)+'.rel')
      self.fileCounter += 1
      self.relationFiles[relId] = fId
      self.fileMap[fId] = \
        self.fileClass(bufferPool=self.bufferPool, \
                       fileId=fId, filePath=path, mode="create", \
                       pageSize=self.defaultPageSize, schema=schema)

      self.checkpoint()

  def addRelation(self, relId, fileId, storageFile):
    if relId not in self.relationFiles and fileId not in self.fileMap:
      self.fileCounter          = max(self.fileCounter, fileId.fileIndex+1)
      self.relationFiles[relId] = fileId
      self.fileMap[fileId]      = storageFile
      self.checkpoint()

  # Removes or detaches a relation from the file manager.
  # When detaching, we do not delete the backing heap file from the file system.
  # This method also removes or detaches any indexes associated with the delation.
  def removeRelation(self, relId, detach=False):
    fId   = self.relationFiles.pop(relId, None)
    rFile = self.fileMap.pop(fId, None) if fId else None
    if rFile and self.indexManager:
      for (_, _, indexId) in self.indexManager.indexes(relId):
        self.indexManager.removeIndex(relId, indexId, detach)

      if not detach:
        rFile.close()
        os.remove(rFile.path)

      self.checkpoint()

  def relationFile(self, relId):
    fId = self.relationFiles.get(relId, None) if relId else None
    return (fId, self.fileMap.get(fId, None)) if fId else (None, None)


  # Page operations
  def readPage(self, pageId, pageBuffer):
    rFile = self.fileMap.get(pageId.fileId, None) if pageId else None
    if rFile:
      return rFile.readPage(pageId, pageBuffer)

  def writePage(self, page):
    rFile = self.fileMap.get(page.pageId.fileId, None) if page.pageId else None
    if rFile:
      return rFile.writePage(page)


  # Index management wrappers.
  def hasIndex(self, relId, keySchema):
    if relId in self.relationFiles and self.indexManager:
      return self.indexManager.hasIndex(relId, keySchema)

  def createIndex(self, relId, relSchema, keySchema, primary):
    if relId in self.relationFiles and self.indexManager:
      return self.indexManager.createIndex(relId, relSchema, keySchema, primary)

  def addIndex(self, relId, relSchema, keySchema, primary, indexId, indexDb):
    if relId in self.relationFiles and self.indexManager:
      self.indexManager.addIndex(relId, relSchema, keySchema, primary, indexId, indexDb)

  def removeIndex(self, relId, indexId):
    if relId in self.relationFiles and self.indexManager:
      self.indexManager.removeIndex(relId, indexId)

  def getIndex(self, indexId):
    if self.indexManager:
      return self.indexManager.getIndex(indexId)

  # Tuple operations

  # Returns a tuple id for the newly inserted data.
  def insertTuple(self, relId, tupleData):
    (_, rFile) = self.relationFile(relId)
    if rFile and self.indexManager:
      tupleId = rFile.insertTuple(tupleData)
      self.indexManager.insertTuple(relId, tupleData, tupleId)
      return tupleId

  def deleteTuple(self, relId, tupleId):
    rFile = self.fileMap.get(tupleId.pageId.fileIndex, None)
    if rFile and self.indexManager:
      tupleData = rFile.deleteTuple(tupleId)
      self.indexManager.deleteTuple(relId, tupleData, tupleId)

  def updateTuple(self, relId, tupleId, tupleData):
    rFile = self.fileMap.get(tupleId.pageId.fileIndex, None)
    if rFile and self.indexManager:
      oldData = rFile.updateTuple(tupleId, tupleData)
      self.indexManager.updateTuple(relId, oldData, tupleData, tupleId)


  # Index-based tuple operations.

  # Perform an index lookup for the given key.
  # This returns an iterator over tuple ids.
  def lookupByIndex(self, relId, indexId, keyData):
    if relId in self.relationFiles and self.indexManager:
      self.indexManager.lookupByIndex(indexId, keyData)

  # Removes tuple(s) by key using the given index.
  # This should maintain all other indexes by retrieving the full tuple and tuple id,
  # and then using the deleteTuple method.
  def deleteByIndex(self, relId, indexId, keyData):
    if relId in self.relationFiles and self.indexManager:
      tupleIds = self.indexManager.lookupByIndex(indexId, keyData)
      for tupleId in tupleIds:
        tupleData = self.deleteTuple(relId, tupleId)
        if tupleData:
          self.indexManager.deleteTuple(relId, tupleData, tupleId)

  # Refreshes tuple(s) by key using the given index.
  # This should support the key value itself changing, and also maintain all other indexes
  # by retrieving the full tuple and tuple id, and then using the updateTuple method.
  def updateByIndex(self, relId, indexId, keyData, tupleData):
    if relId in self.relationFiles and self.indexManager:
      tupleIds = self.indexManager.lookupByIndex(indexId, keyData)
      for tupleId in tupleIds:
        oldData = self.updateTuple(relId, tupleId)
        if oldData:
          self.indexManager.updateTuple(relId, oldData, tupleData, tupleId)

  # Retrieve a tuple based on its key.
  # This method returns None if the relation does not have a primary index,
  # or if the key does not exist in the index.
  # Otherwise it returns a single tuple identifier.
  def lookupByKey(self, relId, keyData):
    if relId in self.relationFiles and self.indexManager:
      self.lookupByKey(relId, keyData)

  # Removes a tuple based on its primary key value.
  def deleteByKey(self, relId, keyData):
    if relId in self.relationFiles and self.indexManager:
      tupleId   = self.indexManager.lookupByKey(relId, keyData)
      tupleData = self.deleteTuple(relId, tupleId)
      if tupleData:
        self.indexManager.deleteTuple(relId, tupleData, tupleId)

  # Updates a tuple based on its primary key value.
  # This should support a change in the key.
  def updateByKey(self, relId, keyData, tupleData):
    if relId in self.relationFiles and self.indexManager:
      tupleId = self.indexManager.lookupByKey(relId, keyData)
      oldData = self.updateTuple(relId, tupleId, tupleData)
      if oldData:
        self.indexManager.updateTuple(relId, oldData, tupleData, tupleId)


  # Tuple-based table scan
  def tuples(self, relId):
    (_, rFile) = self.relationFile(relId)
    if rFile:
      return rFile.tuples()

  # Page-based table scan
  def pages(self, relId):
    (_, rFile) = self.relationFile(relId)
    if rFile:
      return rFile.pages()


  # File manager serialization
  def pack(self):
    if self.relationFiles is not None and self.fileMap is not None:
      pfileClass     = pickle.dumps(self.fileClass).decode(encoding=FileManager.checkpointEncoding)
      prelationFiles = list(map(lambda entry: (entry[0], entry[1].fileIndex), self.relationFiles.items()))
      pfileMap       = list(map(lambda entry: (entry[0].fileIndex, entry[1].path), self.fileMap.items()))
      return json.dumps((self.dataDir, self.indexDir, pfileClass, self.fileCounter, prelationFiles, pfileMap))

  @classmethod
  def unpack(cls, bufferPool, strBuffer):
    args = json.loads(strBuffer)
    if len(args) == 6:
      unfileClass = pickle.loads(args[2].encode(encoding=FileManager.checkpointEncoding))
      return cls(bufferPool=bufferPool, dataDir=args[0], indexDir=args[1], \
                 fileClass=unfileClass, fileCounter=args[3], restore=(args[4], args[5]))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
