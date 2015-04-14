import json, io, os, os.path

from Catalog.Schema        import DBSchema, DBSchemaEncoder, DBSchemaDecoder
from Query.Plan            import PlanBuilder
from Query.Optimizer       import Optimizer
from Storage.StorageEngine import StorageEngine

class Database:
  """
  A top-level database engine class.

  For now, this primarily maintains a simple catalog,
  mapping relation names to schema objects.

  Also, it provies the ability to construct query
  plan objects, as well as wrapping the storage layer methods.
  """

  checkpointEncoding = "latin1"
  checkpointFile     = "db.catalog"

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      storageArgs = {k:v for (k,v) in kwargs.items() \
                      if k in ["pageSize", "poolSize", "dataDir", "indexDir"]}

      self.relationMap     = kwargs.get("relations", {})
      self.defaultPageSize = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
      self.storage         = kwargs.get("storage", StorageEngine(**storageArgs))
      self.optimizer       = Optimizer(self)

      checkpointFound = os.path.exists(os.path.join(self.storage.fileMgr.dataDir, Database.checkpointFile))
      restoring       = "restore" in kwargs

      if not restoring and checkpointFound:
        self.restore()

  def fromOther(self, other):
    self.relationMap     = other.relationMap
    self.defaultPageSize = other.defaultPageSize
    self.storage         = other.storage
    self.optimizer       = other.optimizer

  def close(self):
    if self.storage:
      self.storage.close()

  # Database internal components
  def storageEngine(self):
    return self.storage

  def bufferPool(self):
    return self.storage.bufferPool if self.storage else None

  def fileManager(self):
    return self.storage.fileMgr if self.storage else None

  def queryOptimizer(self):
    return self.optimizer

  # User API

  # Catalog methods
  def relations(self):
    return self.relationMap.keys()

  def hasRelation(self, relationName):
    return relationName in self.relationMap

  def relationSchema(self, relationName):
    if relationName in self.relationMap:
      return self.relationMap[relationName]

  # DDL statements
  def createRelation(self, relationName, relationFields):
    if relationName not in self.relationMap:
      schema = DBSchema(relationName, relationFields)
      self.relationMap[relationName] = schema
      self.storage.createRelation(relationName, schema)
      self.checkpoint()
    else:
      raise ValueError("Relation '" + relationName + "' already exists")

  def removeRelation(self, relationName):
    if relationName in self.relationMap:
      del self.relationMap[relationName]
      self.storage.removeRelation(relationName)
      self.checkpoint()
    else:
      raise ValueError("No relation '" + relationName + "' found in database")

  # DML statements

  # Returns a tuple id for the newly inserted data.
  def insertTuple(self, relationName, tupleData):
    if relationName in self.relationMap:
      return self.storage.insertTuple(relationName, tupleData)
    else:
      raise ValueError("Unknown relation '" + relationName + "' while inserting a tuple")

  def deleteTuple(self, tupleId):
    self.storage.deleteTuple(tupleId)

  def updateTuple(self, tupleId, tupleData):
    self.storage.updateTuple(tupleId, tupleData)

  # Queries

  # Returns an empty query builder that can access the current database.
  def query(self):
    return PlanBuilder(db=self)

  # Returns an iterable for query results, after initializing the given plan.
  def processQuery(self, queryPlan):
    return queryPlan.prepare(self)

  # Returns an optimized version of the given query plan.
  def optimizeQuery(self, queryPlan):
    return optimizer.optimizeQuery(queryPlan)

  # Save the database internals to the data directory.
  def checkpoint(self):
    if self.storage:
      dbcPath = os.path.join(self.storage.fileMgr.dataDir, Database.checkpointFile)
      with open(dbcPath, 'w', encoding=Database.checkpointEncoding) as f:
        f.write(self.pack())

  # Load relations and schema from an existing data directory.
  def restore(self):
    if self.storage:
      dbcPath = os.path.join(self.storage.fileMgr.dataDir, Database.checkpointFile)
      with open(dbcPath, 'r', encoding=Database.checkpointEncoding) as f:
        other = Database.unpack(f.read(), self.storage)
        self.fromOther(other)

  # Database schema catalog serialization
  def pack(self):
    if self.relationMap is not None:
      return json.dumps([self.relationMap, self.defaultPageSize], cls=DBSchemaEncoder)

  @classmethod
  def unpack(cls, buffer, storageEngine):
    (relationMap, pageSize) = json.loads(buffer, cls=DBSchemaDecoder)
    return cls(relations=relationMap, pageSize=pageSize, storage=storageEngine, restore=True)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
