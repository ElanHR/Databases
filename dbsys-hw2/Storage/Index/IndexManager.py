import json, os, os.path

from bsddb3              import db
from Catalog.Schema      import DBSchema, DBSchemaEncoder, DBSchemaDecoder
from Catalog.Identifiers import FileId, PageId, TupleId

class IndexManager:
  """
  An index manager class.

  This provides indexes on top of the heap files in our storage layer with BerkeleyDB.
  Each index object is a BerkeleyDB database object whose values are tuple identifiers
  objects. In this way, each index is an unclustered index that must perform a random
  I/O operation to subsequently retrieve the page and tuple from our storage layer.

  The index manager class provides facilities to create and remove both primary and
  secondary indexes. Keys for a primary indexes must be unique, while secondary indexes
  may optionally specify unique or non-unique keys. A relation can have at most one
  primary index.

  The index manager maintains two internal data structures: relationIndexes and indexMap.
  The latter is a dictionary mapping an index id to a BerkeleyDB object.
  The former is a dictionary mapping a relation name to a triple of relation schema, 
  primary index id and key schema, and a dictionary of secondary index ids by
  their key schema. Index ids are returned on index construction and must be used
  to retrieve the index object.

  Indexes provide both scan and lookup operations, as well as modifications.

  Index maintenance is performed through the insertTuple, deleteTuple and updateTuple
  methods. These are invoked on indexes by the file manager when modifying the
  underyling storage file, to ensure the indexes are kept consistent. 
  These methods ensure that all indexes (both primary and secondaries) are maintained.

  In a similar fashion to the file manager, the index manager checkpoints its
  internal data structures to disk.

  >>> im = IndexManager()

  ## Test low-level BDB database operations
  
  # Test index creation
  >>> indexDb = im.createIndexDB('test.db')
  >>> indexDb.get_dbname()
  ('test.db', None)

  # Test index close and reopen
  >>> im.closeIndexDB(indexDb)
  >>> indexDb2 = im.openIndexDB('test.db')
  >>> indexDb2.get_dbname()
  ('test.db', None)

  # Test index removal
  >>> im.removeIndexDB(indexDb2)

  ## Test index operations
  >>> schema    = DBSchema('employee', [('id', 'int'), ('age', 'int'), ('salary', 'double')])
  >>> keySchema = DBSchema('employeeKey', [('id', 'int')])
  >>> ageSchema = DBSchema('employeeAge', [('age', 'int')])

  # Test index addition
  >>> indexId1 = im.createIndex(schema.name, schema, keySchema, True)
  >>> indexId2 = im.createIndex(schema.name, schema, ageSchema, False)
  
  >>> im.indexes(schema.name) # doctest:+ELLIPSIS
  [(..., True, 1), (..., False, 2)]

  >>> im.hasIndex(schema.name, keySchema)
  True
  
  >>> im.hasIndex(schema.name, ageSchema)
  True

  # Test index retrieval
  >>> im.getIndex(indexId1).get_dbname()
  ('employee_idx1', None)

  >>> im.getIndex(indexId2).get_dbname()
  ('employee_idx2', None)

  # Test index matching
  >>> im.matchIndex(schema.name, DBSchema('foo', [('age', 'int')]))
  2

  ## Data operations: test data insertion/deletion/lookup on all indexes

  # Insert a tuple
  >>> pageId = PageId(FileId(0), 1)
  >>> e1Id = TupleId(pageId, 1000)
  >>> e1Data = schema.pack(schema.instantiate(1, 25, 100000))
  >>> im.insertTuple(schema.name, e1Data, e1Id)

  # Look up that tuple in both indexes
  >>> idx1Key = schema.projectBinary(e1Data, keySchema)
  >>> [(tId.pageId.pageIndex, tId.tupleIndex) \
        for tId in im.lookupByIndex(indexId1, idx1Key)]
  [(1, 1000)]

  >>> idx2Key = schema.projectBinary(e1Data, ageSchema)
  >>> [(tId.pageId.pageIndex, tId.tupleIndex) \
        for tId in im.lookupByIndex(indexId2, idx2Key)]
  [(1, 1000)]

  # Update the tuple contents, changing the age field.
  # This should cause no change in the primary index (by employee id),
  # but should invalidate the secondary index entry (based on age).
  >>> e1NewDataNewKey = schema.pack(schema.instantiate(1, 30, 90000))
  >>> im.updateTuple(schema.name, e1Data, e1NewDataNewKey, e1Id)

  # Look up the old tuple in both indexes
  >>> idx1Key = schema.projectBinary(e1Data, keySchema)
  >>> [(tId.pageId.pageIndex, tId.tupleIndex) \
        for tId in im.lookupByIndex(indexId1, idx1Key)]
  [(1, 1000)]

  >>> idx2Key = schema.projectBinary(e1Data, ageSchema)
  >>> list(im.lookupByIndex(indexId2, idx2Key))
  []

  # Look up the new tuple in both indexes
  >>> idx1Key = schema.projectBinary(e1NewDataNewKey, keySchema)
  >>> [(tId.pageId.pageIndex, tId.tupleIndex) \
        for tId in im.lookupByIndex(indexId1, idx1Key)]
  [(1, 1000)]

  >>> idx2Key = schema.projectBinary(e1NewDataNewKey, ageSchema)
  >>> [(tId.pageId.pageIndex, tId.tupleIndex) \
        for tId in im.lookupByIndex(indexId2, idx2Key)]
  [(1, 1000)]

  # Delete an indexed tuple
  >>> im.deleteTuple(schema.name, e1NewDataNewKey, e1Id)

  # Ensure that the lookup returns no tuples.
  >>> idx1Key = schema.projectBinary(e1NewDataNewKey, keySchema)
  >>> list(im.lookupByIndex(indexId1, idx1Key))
  []

  >>> idx2Key = schema.projectBinary(e1NewDataNewKey, ageSchema)
  >>> list(im.lookupByIndex(indexId2, idx2Key))
  []

  ## Index scan tests

  # Add many tuples.
  >>> testTuples = []
  >>> for i in range(10):
  ...    dataIdPair = (schema.pack(schema.instantiate(i, 2*i+20, 5000*(10+i))), TupleId(pageId, i))
  ...    testTuples.append(dataIdPair)
  ...

  >>> for (tup, tupId) in testTuples:
  ...    _ = im.insertTuple(schema.name, tup, tupId)
  ...

  # Scan by both indexes, ensuring they are sorted on their search key.
  >>> [keySchema.unpack(k).id for (k,_) in im.scanByIndex(indexId1)] # doctest:+ELLIPSIS
  [0, 1, 2, ..., 9]

  >>> [ageSchema.unpack(k).age for (k,_) in im.scanByIndex(indexId2)] # doctest:+ELLIPSIS
  [20, 22, 24, ..., 38]


  # Test index removal
  >>> im.removeIndex(schema.name, indexId1)
  >>> im.indexes(schema.name) # doctest:+ELLIPSIS
  [(..., False, 2)]

  >>> im.removeIndex(schema.name, indexId2)
  >>> im.indexes(schema.name) # doctest:+ELLIPSIS
  []

  """

  defaultIndexDir = "data/index"

  checkpointEncoding = "latin1"
  checkpointFile     = "db.im"

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      self.indexDir   = kwargs.get("indexDir", IndexManager.defaultIndexDir)
      checkpointFound = os.path.exists(os.path.join(self.indexDir, IndexManager.checkpointFile))
      restoring       = "restore" in kwargs

      if not os.path.exists(self.indexDir):
          os.makedirs(self.indexDir)

      if restoring or not checkpointFound:
        self.indexCounter    = kwargs.get("indexCounter", 0)
        self.relationIndexes = kwargs.get("relationIndexes", {}) # rel id -> (relation schema, primary, dict(secondaries))
        self.indexMap        = kwargs.get("indexMap", {})        # index id -> DB object

        self.initializeDB(self.indexDir)

        if restoring:
          # Initialize relationIndexes and indexMap from restore data.
          for i in kwargs["restore"][0]:
            self.relationIndexes[i[0]] = (i[1][0], i[1][1], dict(i[1][2]))

          for i in kwargs["restore"][1]:
            self.indexMap[i[0]] = self.openIndexDB(i[1])        
      
      else:
        self.restore()

  def fromOther(self, other):
    self.indexDir        = other.indexDir
    self.indexCounter    = other.indexCounter
    self.relationIndexes = other.relationIndexes
    self.indexMap        = other.indexMap
    self.env             = other.env

  # Save the index manager internals to the data directory.
  def checkpoint(self):
    imPath = os.path.join(self.indexDir, IndexManager.checkpointFile)
    with open(imPath, 'w', encoding=IndexManager.checkpointEncoding) as f:
      f.write(self.pack())

  # Load indexes from an existing data directory.
  def restore(self):
    imPath = os.path.join(self.indexDir, IndexManager.checkpointFile)
    with open(imPath, 'r', encoding=IndexManager.checkpointEncoding) as f:
      other = IndexManager.unpack(f.read())
      self.fromOther(other)


  # Berkeley DB utility methods.

  # Initializes a new BerkeleyDB environment and database to store a set of indexes.
  def initializeDB(self, dbDir):
    self.env = db.DBEnv()
    envFlags = db.DB_CREATE | db.DB_INIT_MPOOL
    self.env.open(dbDir, envFlags)

  # TODO: set duplicate flags for secondary indexes as needed.
  def createIndexDB(self, filename):
    indexDb = db.DB(dbEnv=self.env)
    dbFlags = db.DB_CREATE | db.DB_TRUNCATE
    indexDb.open(filename, db.DB_BTREE, dbFlags)
    return indexDb

  def openIndexDB(self, filename):
    indexDb = db.DB(dbEnv=self.env)
    indexDb.open(filename, db.DB_BTREE)
    return indexDb

  def closeIndexDB(self, indexDb):
    indexDb.close()

  def removeIndexDB(self, indexDb):
    filename, _ = indexDb.get_dbname()
    self.closeIndexDB(indexDb)
    self.env.dbremove(filename)


  # Index identifier methods.
  
  def indexFileName(self, relId, indexId):
    return relId+"_idx"+str(indexId)

  # Generates a filename for the index.
  def generateIndexFileName(self, relId):
    self.indexCounter += 1
    return (self.indexCounter, self.indexFileName(relId, self.indexCounter))


  # Index management methods

  # Returns whether the relation has any indexes initialized.
  def hasIndexes(self, relId):
    return relId in self.relationIndexes and self.relationIndexes[relId]

  # Returns the indexes available on a relation as a triple of (schema, primary, index id)
  def indexes(self, relId):
    if self.hasIndexes(relId):
      _, primary, secondaries = self.relationIndexes[relId]
      firstElem = [(primary[0], True, primary[1])] if primary else []
      return firstElem + list(map(lambda x: (x[0], False, x[1]), secondaries.items()))
    return []

  # Returns whether an index on the given key exists for a relation.
  # Key schemas are treated as lists rather than sets, that is a schema
  # with the same ordering of attributes must exist in the IndexManager.
  def hasIndex(self, relId, keySchema):
    if self.hasIndexes(relId):
      _, primary, secondaries = self.relationIndexes[relId]
      return (primary is not None and primary[0] == keySchema) or keySchema in secondaries
    return False

  def checkDuplicateIndex(self, relId, keySchema, primary):
    errorMsg = None
    if self.hasIndexes(relId):
      _, prim, _ = self.relationIndexes[relId]
      
      if primary and prim is not None:
        errorMsg = "Invalid construction of a duplicate primary index"
      
      elif self.hasIndex(relId, keySchema):
        errorMsg = "Invalid construction of a duplicate index"

    return errorMsg

  # Creates a new index for the given key as a BDB database.
  # Returns the index id of a newly created index from key -> relation
  # If the index is indicated to be a primary index, the values are tuple identifiers,
  # while for secondary indexes, the values are sets of tuple identifiers.
  # This method should ensure that no relation has two primary indexes.
  def createIndex(self, relId, relSchema, keySchema, primary):
    # Check if this is a duplicate index and abort.
    errorMsg = self.checkDuplicateIndex(relId, keySchema, primary)
    if errorMsg:
      raise ValueError(errorMsg)

    indexId, indexFile = self.generateIndexFileName(relId)
    indexDb = self.createIndexDB(indexFile)
    self.indexMap[indexId] = indexDb

    # Add the new index to the relationFiles data structure.
    if primary:
      schema, _, secondaries = \
        self.relationIndexes[relId] if self.hasIndexes(relId) else (relSchema, None, {})
      
      self.relationIndexes[relId] = (schema, (keySchema, indexId), secondaries)
    
    else:
      if not self.hasIndexes(relId):
        self.relationIndexes[relId] = (relSchema, None, {})
      self.relationIndexes[relId][2][keySchema] = indexId

    self.checkpoint()
    return indexId


  # Adds a pre-existing BDB index to the database.
  def addIndex(self, relId, relSchema, keySchema, primary, indexId, indexDb):
    if indexId not in self.indexMap:
      # Check if this is a duplicate index and abort.
      errorMsg = self.checkDuplicateIndex(relId, keySchema, primary)
      if errorMsg:
        raise ValueError(errorMsg)

    self.indexCounter = max(self.indexCounter, indexId+1)
    self.indexMap[indexId] = indexDb

    # Add the new index to the relationFiles data structure.
    if primary:
      schema, _, secondaries = \
        self.relationIndexes[relId] if self.hasIndexes(relId) else (relSchema, None, {})
      
      self.relationIndexes[relId] = (schema, (keySchema, indexId), secondaries)
    
    else:
      if not self.hasIndexes(relId):
        self.relationIndexes[relId] = (relSchema, None, {})
      self.relationIndexes[relId][2][keySchema] = indexId

    self.checkpoint()


  # Returns the index (i.e., BDB database object) corresponding to the index id.
  def getIndex(self, indexId):
    if indexId in self.indexMap:
      return self.indexMap[indexId]

  # Removes or detaches the index (i.e., BDB database object) for the given relation.
  def removeIndex(self, relId, indexId, detach=False):
    if self.hasIndexes(relId):
      schema, primary, secondaries = self.relationIndexes[relId]
      if primary and primary[1] == indexId:
        self.relationIndexes[relId] = (schema, None, secondaries)
      else:
        self.relationIndexes[relId] = \
          (schema, primary, dict(filter(lambda x: x[1] != indexId, secondaries.items())))

      # Clean up relationIndexes entries when no primary or secondary is present.
      if self.relationIndexes[relId][1] is None and not self.relationIndexes[relId][2]:
        del self.relationIndexes[relId]

    if indexId in self.indexMap:
      indexDb = self.indexMap.pop(indexId, None)
      if indexDb and detach:
        self.closeIndexDB(indexDb)
      elif indexDb:
        self.removeIndexDB(indexDb)

    self.checkpoint()

  # Returns the index id of the best matching index
  # For now, this requires an exact match on the schema fields and types, but not the name.
  def matchIndex(self, relId, keySchema):
    indexes = self.indexes(relId)
    if indexes:
      return next((x[2] for x in indexes if keySchema.match(x[0])), None)

  # Auxiliary index helpers.

  def hasPrimaryIndex(self, relId):
    return self.hasIndexes(relId) and self.relationIndexes[relId][1] is not None

  def getPrimaryIndex(self, relId):
    if self.hasIndexes(relId):
      _, primary, _ = self.relationIndexes[relId]
      return self.getIndex(primary[0])


  # Index access methods.

  # Updates all indexes on the relation to add the new tuple.
  # The key for each index should be extracted from the full tuple given in tupleData.
  def insertTuple(self, relId, tupleData, tupleId):
    if self.hasIndexes(relId):
      schema, _, _ = self.relationIndexes[relId]
      indexes      = self.indexes(relId)
      if indexes:
        for (keySchema, primary, indexId) in indexes:
          indexDb  = self.getIndex(indexId)
          if indexDb is not None:
            indexKey = schema.projectBinary(tupleData, keySchema)
            putFlags = db.DB_NOOVERWRITE if primary else 0
            indexDb.put(indexKey, tupleId.pack(), flags=putFlags)

  # Updates all indexes on the relation to remove the given tuple.
  # The key for each index should be extracted from the full tuple given in tupleData.
  def deleteTuple(self, relId, tupleData, tupleId):
    if self.hasIndexes(relId):
      schema, _, _ = self.relationIndexes[relId]
      indexes      = self.indexes(relId)
      if indexes:
        for (keySchema, primary, indexId) in indexes:
          indexDb  = self.getIndex(indexId)
          if indexDb is not None:
            indexKey = schema.projectBinary(tupleData, keySchema)
            if primary:
              indexDb.delete(indexKey)
            else:
              # Delete only the tuple matching the given tuple id.
              crsr = indexDb.cursor()
              found = crsr.get_both(indexKey, tupleId.pack())
              if found:
                crsr.delete()
              crsr.close()

  # Updates all indexes on the relation to refresh the given tuple.
  # The old and new keys for each index should be extracted from the full tuples.
  # For each index, based on whether the key is changing, this method should issue
  # the appropriate DB delete+insert calls.
  # Note: since our storage engine uses heap files only, the tuple id itself should not change.
  def updateTuple(self, relId, oldData, newData, tupleId):
    if self.hasIndexes(relId):
      schema, _, _ = self.relationIndexes[relId]
      indexes      = self.indexes(relId)
      if indexes:
        for (keySchema, primary, indexId) in indexes:
          indexDb = self.getIndex(indexId)
          if indexDb is not None:
            oldKey  = schema.projectBinary(oldData, keySchema)
            newKey  = schema.projectBinary(newData, keySchema)

            # If the keys are the same, we do not need to perform any operations.
            # That is, we assume the tuple id argument is the same as the existing
            # entry (since this is a tuple id), and we do not check this.
            if oldKey == newKey:
              pass

            # Insert a new index entry if the key has changed.
            else:
              if primary:
                indexDb.delete(oldKey)
                indexDb.put(newKey, tupleId.pack(), flags=db.DB_NOOVERWRITE)
              else:
                # Update only the tuple matching the given tuple id.
                crsr = indexDb.cursor()
                found = crsr.get_both(oldKey, tupleId.pack())
                if found:
                  crsr.delete()
                  crsr.put(newKey, tupleId.pack(), flags=db.DB_KEYLAST)
                    # TODO: flags based on whether the secondary index is unique?
                crsr.close()


  # Lookup methods.

  # Perform an index lookup for the given key.
  # This returns an iterator over tuple ids.
  # TODO: this materializes all tuple ids.
  # TODO: Use an iterator abstraction to avoid this.
  def lookupByIndex(self, indexId, keyData):
    result = []
    indexDb = self.getIndex(indexId)
    if indexDb is not None:
      crsr = indexDb.cursor()
      
      data = crsr.set(keyData)
      while data and data[0] == keyData:
        result.append(TupleId.unpack(data[1]))
        data = crsr.next()

      crsr.close()
      return iter(result)

  # Retrieve a tuple based on its key.
  # This method returns None if the relation does not have a primary index,
  # or if the key does not exist in the index.
  # Otherwise it returns a single tuple identifier.
  def lookupByKey(self, relId, keyData):
    indexDb = self.getPrimaryIndex(relId)
    if indexDb:
      return TupleId.unpack(indexDb.get(keyData))


  # Index scan operations.
  # These return an ordered iterator of (key, tuple id) pairs

  # Scan over a specific index.
  def scanByIndex(self, indexId):
    indexDb = self.getIndex(indexId)
    if indexDb is not None:
      return iter(indexDb.items())

  # Scan over the primary index for a relation.
  def scanByKey(self, relId):
    indexDb = self.getPrimaryIndex(relId)
    if indexDb is not None:
      return iter(indexDb.items())


  # Index manager serialization
  def packSchema(self, schema):
    return (schema.name, schema.schema())

  def pack(self):
    if self.relationIndexes is not None and self.indexMap is not None:
      # Convert secondaries dictionary to a list since it has an object as a key type (incompatible w/ JSON)
      pRelIndexes = list(map(lambda x: (x[0], (x[1][0], x[1][1], list(x[1][2].items()))), self.relationIndexes.items()))
      pIndexMap   = list(map(lambda entry: (entry[0], entry[1].get_dbname()), self.indexMap.items()))
      return json.dumps((self.indexDir, self.indexCounter, pRelIndexes, pIndexMap), cls=DBSchemaEncoder)

  @classmethod
  def unpack(cls, buffer):
    args = json.loads(buffer, cls=DBSchemaDecoder)
    if len(args) == 4:
      return cls(indexDir=args[0], indexCounter=args[1], restore=(args[2], args[3]))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
