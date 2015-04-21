class Operator:
  """
  An abstract base class for all operator implementations.
  This describes the API that all operators should provide, and also
  provides operator identifiers.
  """

  opCount = 0

  def __init__(self, **kwargs):
    self.opId = Operator.opCount
    Operator.opCount += 1
    self.pipelined    = kwargs.get("pipeline", False)
    self.sampled      = kwargs.get("sampled", False)
    self.sampleFactor = kwargs.get("sampleFactor", 1.0)
    self.tupleCost    = kwargs.get("tupleCost", 1.0)
    self.initializeStatistics()

  def initializeStatistics(self):
    self.estimatedCardinality = 0
    self.actualCardinality    = 0

  # Returns this operator's identifier.
  def id(self):
    return self.opId

  # Returns the output schema of this operator
  def schema(self):
    raise NotImplementedError

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    raise NotImplementedError

  # Returns a string describing the operator type
  def operatorType(self):
    return "Operator"

  # Returns child operators if present
  def inputs(self):
    raise NotImplementedError

  # Prepares the operator for execution.
  def prepare(self, database):
    self.storage = database.storageEngine()

  # Create a temporary output relation, removing any existing relation.
  def initializeOutput(self):
    relId = self.relationId()

    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)

    self.storage.createRelation(relId, self.schema())
    self.tempFile = self.storage.fileMgr.relationFile(relId)[1]
    self.outputPages = []

  # Returns an identifier for this operator's output relation
  def relationId(self):
    return "tmp_" + self.operatorType() + "_" + str(self.id())

  # Python implementation of Volcano-style iterator abstraction
  def __iter__(self):
    raise NotImplementedError

  def __next__(self):
    raise NotImplementedError

  # Page processing and control methods

  # Used during operator processing to indicate a new output tuple.
  # The operator implementation must store this tuple in an output page, allocating a
  # new output page as necessary.
  def emitOutputTuple(self, tupleData):
    if self.tempFile is None:
      self.initializeOutput()

    allocatePage = not(self.outputPages and self.outputPages[-1][1].header.hasFreeTuple())
    if allocatePage:
      # Flush the most recently updated output page, which updates the storage file's
      # free page list to ensure correct new page allocation.
      if self.outputPages:
        self.storage.bufferPool.flushPage(self.outputPages[-1][0])
      outputPageId = self.tempFile.availablePage()
      outputPage   = self.storage.bufferPool.getPage(outputPageId)
      self.outputPages.append((outputPageId, outputPage))
    else:
      outputPage = self.outputPages[-1][1]

    outputPage.insertTuple(tupleData)

    if self.sampled:
      self.estimatedCardinality += 1
    else:
      self.actualCardinality += 1

  # Returns whether this operator has an output page ready for its iterator.
  # This method can raise a StopIteration exception to end this operator's processing.
  def isOutputPageReady(self):
    numOutputs = len(self.outputPages)
    if numOutputs > 0:
      return not(self.outputPages[0][1].header.hasFreeTuple()) if numOutputs == 1 else True
    return False

  # Returns the next output page for this operator's iterator.
  # This method must raise a StopIteration exception when no output pages are available.
  def outputPage(self):
    if self.outputPages:
      return self.outputPages.pop(0)
    raise StopIteration

  # Page-at-a-time operator processing
  # This method can raise a StopIteration exception to end this operator's processing.
  def processInputPage(self, pageId, page):
    raise NotImplementedError

  # Set-at-a-time operator processing
  def processAllPages(self):
    raise NotImplementedError

  # Expression evaluation methods.

  # Loads (i.e., binds) all the fields in the given schema and tuple
  # into the Python environment, making them accessible as local
  # Python variables.
  # Query operator expressions (e.g., where-clauses, select lists, join
  # expressions) can then be evaluated in this environment.
  def loadSchema(self, schema, tupleData):
    schemaLocals = {}
    for ((k,t),v) in zip(schema.schema(), schema.unpack(tupleData)):
      schemaLocals[k] = v
    return schemaLocals

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return self.operatorType() + "[" + str(self.id()) + ",cost={:.2f}".format(self.cost(True)) + "]"

  # Instructs this operator to perform sampling during execution.
  # This propagates the sampling rate over all of our children.
  def useSampling(self, sampled, sampleFactor):
    self.sampled = sampled
    self.sampleFactor = sampleFactor
    for childOp in self.inputs():
      childOp.useSampling(sampled, sampleFactor)

  # Returns the number of tuples this operator produces, either
  # as an estimate or a profiled actual cardinality.
  def cardinality(self, estimated):
    if estimated:
      return self.estimatedCardinality * self.sampleFactor
    else:
      return self.actualCardinality

  # Returns this operator's selectivity, either as an estimate or
  # a profiled actual selectivity.
  def selectivity(self, estimated):
    numInputs = sum(map(lambda x: x.cardinality(estimated), self.inputs()))
    numOutputs = self.cardinality(estimated)
    return numOutputs / numInputs

  # Returns the cost of this operator in terms of a dimensionless
  # metric (e.g., number of I/Os, CPU cycles, etc). This is also either
  # as an estimate or a profiled actual cost.
  def cost(self, estimated):
    subPlanCost = sum(map(lambda x: x.cost(estimated), self.inputs()))
    return self.localCost(estimated) + subPlanCost

  def localCost(self, estimated):
    numInputs = sum(map(lambda x: x.cardinality(estimated), self.inputs()))
    return numInputs * self.tupleCost
