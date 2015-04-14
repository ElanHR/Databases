from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    self.partitionFiles = {}
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    return next(self.outputIterator)


  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Processing helpers
  def ensureTuple(self, x):
    if not isinstance(x, tuple):
      return (x,)
    else:
      return x

  def initialExprs(self):
    return [i[0] for i in self.aggExprs]

  def incrExprs(self):
    return [i[1] for i in self.aggExprs]

  def finalizeExprs(self):
    return [i[2] for i in self.aggExprs]

  # Set-at-a-time operator processing
  def processAllPages(self):
    # Create partitions of the input records by hashing the group-by values
    for (pageId, page) in iter(self.subPlan):
      for tup in page:
        groupVal = self.ensureTuple(self.groupExpr(self.subSchema.unpack(tup)))
        groupId = self.groupHashFn(groupVal)
        self.emitPartitionTuple(groupId, tup)

    # We assume that the partitions fit in main memory.
    for partRelId in self.partitionFiles.values():
      partFile = self.storage.fileMgr.relationFile(partRelId)[1]

      # Use an in-memory Python dict to accumulate the aggregates.
      aggregates = {}
      for (pageId, page) in partFile.pages():
        for tup in page:
          # Evaluate group-by value.
          namedTup = self.subSchema.unpack(tup)
          groupVal = self.ensureTuple(self.groupExpr(namedTup))

          # Look up the aggregate for the group.
          if groupVal not in aggregates:
            aggregates[groupVal] = self.initialExprs()

          # Increment the aggregate.
          aggregates[groupVal] = \
            list(map( \
              lambda x: x[0](x[1], namedTup), \
              zip(self.incrExprs(), aggregates[groupVal])))

      # Finalize the aggregate value for each group.
      for (groupVal, aggVals) in aggregates.items():
        finalVals = list(map(lambda x: x[0](x[1]), zip(self.finalizeExprs(), aggVals)))
        outputTuple = self.outputSchema.instantiate(*(list(groupVal) + finalVals))
        self.emitOutputTuple(self.outputSchema.pack(outputTuple))

      # No need to track anything but the last output page when in batch mode.
      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    # Clean up partitions.
    self.removePartitionFiles()

    # Return an iterator for the output file.
    return self.storage.pages(self.relationId())

  # Bucket construction helpers.
  def partitionRelationId(self, partitionId):
    return self.operatorType() + str(self.id()) + "_" \
            + "part_" + str(partitionId)

  def emitPartitionTuple(self, partitionId, partitionTuple):
    partRelId  = self.partitionRelationId(partitionId)

    # Create a partition file as needed.
    if not self.storage.hasRelation(partRelId):
      self.storage.createRelation(partRelId, self.subSchema)
      self.partitionFiles[partitionId] = partRelId

    partFile = self.storage.fileMgr.relationFile(partRelId)[1]
    if partFile:
      partFile.insertTuple(partitionTuple)

  # Delete all existing partition files.
  def removePartitionFiles(self):
    for partRelId in self.partitionFiles.values():
      self.storage.removeRelation(partRelId)
    self.partitionFiles = {}


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
