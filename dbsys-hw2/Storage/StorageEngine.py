from Catalog.Schema      import DBSchema
from Storage.FileManager import FileManager
from Storage.BufferPool  import BufferPool

class StorageEngine:
  """
  A storage engine implementation, containing a file manager and buffer pool.

  This provides a high-level API to operate on database relations and tuples
  based on the functionality provided by the buffer pool, file manager and
  the remaining components of the storage engine.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])

  >>> storage = StorageEngine()
  >>> list(storage.relations())
  []

  # Create employee relation
  >>> storage.createRelation(schema.name, schema)

  # Populate relation
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(20)]:
  ...    _ = storage.insertTuple(schema.name, tup)
  ...

  # Test table scan
  >>> [schema.unpack(tup).id for tup in storage.tuples(schema.name)] == list(range(20))
  True

  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      bpArgs          = {k:v for (k,v) in kwargs.items() if k in ["pageSize", "poolSize"]}
      fmArgs          = {k:v for (k,v) in kwargs.items() if k in ["pageSize", "dataDir", "indexDir"]}
      self.bufferPool = BufferPool(**bpArgs)
      self.fileMgr    = FileManager(bufferPool=self.bufferPool, **fmArgs)

      if self.fileMgr:
        self.bufferPool.setFileManager(self.fileMgr)

  def fromOther(self, other):
    self.bufferPool = other.bufferPool
    self.fileMgr    = other.fileMgr

  def close(self):
    if self.fileMgr:
      self.fileMgr.close()

  # Data definition operations

  def relations(self):
    if self.fileMgr:
      return self.fileMgr.relations()

  def hasRelation(self, relId):
    if self.fileMgr:
      return self.fileMgr.hasRelation(relId)

  def createRelation(self, relId, schema):
    if self.fileMgr:
      self.fileMgr.createRelation(relId, schema)
    else:
      raise ValueError("Could not create relation, no file manager found")

  def removeRelation(self, relId):
    if self.fileMgr:
      self.fileMgr.removeRelation(relId)
    else:
      raise ValueError("Could not remove relation, no file manager found")

  def hasIndex(self, relId, keySchema):
    if self.fileMgr:
      return self.fileMgr.hasIndex(relId, keySchema)

  def createIndex(self, relId, relSchema, keySchema, primary):
    if self.fileMgr:
      return self.fileMgr.createIndex(relId, relSchema, keySchema, primary)
  
  def addIndex(self, relId, relSchema, keySchema, primary, indexId, indexDb):
    if self.fileMgr:
      self.fileMgr.addIndex(relId, relSchema, keySchema, primary, indexId, indexDb)

  def removeIndex(self, relId, indexId):
    if self.fileMgr:
      self.fileMgr.removeIndex(relId, indexId)

  def getIndex(self, indexId):
    if self.fileMgr:
      return self.fileMgr.getIndex(indexId)


  # Data manipulation operations

  # Returns a tuple id for the newly inserted data.
  def insertTuple(self, relId, tupleData):
    if self.fileMgr:
      return self.fileMgr.insertTuple(relId, tupleData)
    else:
      raise ValueError("Could not insert tuple, no file manager found")

  def deleteTuple(self, relId, tupleId):
    if self.fileMgr:
      self.fileMgr.deleteTuple(relId, tupleId)
    else:
      raise ValueError("Could not delete tuple, no file manager found")

  def updateTuple(self, relId, tupleId, tupleData):
    if self.fileMgr:
      self.fileMgr.updateTuple(relId, tupleId, tupleData)
    else:
      raise ValueError("Could not update tuple, no file manager found")

  # Tuple-based table scan
  def tuples(self, relId):
    if self.fileMgr:
      return self.fileMgr.tuples(relId)

  # Page-based table scan
  def pages(self, relId):
    if self.fileMgr:
      return self.fileMgr.pages(relId)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
