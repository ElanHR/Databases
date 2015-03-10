from Catalog.Schema import DBSchema
from Query.Operator import Operator

'''
query6 = db.query().fromTable('employee').groupBy( \
          groupSchema=keySchema, \
          aggSchema=aggMinMaxSchema, \
          groupExpr=(lambda e: e.id), \
          aggExprs=[ (sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), \
                    (0, lambda acc, e: max(acc, e.age), lambda x: x) ], \
          groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
        ).finalize()
'''

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

    raise NotImplementedError

  def __next__(self):

    raise NotImplementedError


  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for group-by")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)

    self.initializeOutput()
    # outputPage = self.outputPages[0]

    # initialize each partition
    # for tup in outputPage:
    #   tup = self.aggExprs[0]

    groupDict = dict()
    # Process all pages from the child operator.
    try:
      # Loop through all tuples
      for (pageId, page) in self.inputIterator: 
        for inputTuple in page:

          # Partition = hashFn( groupSchema(tuple) )
          # aggregate = lambda( aggreSchema(tuple) )
          # (Partition, Group) = aggragate

          groupVals = self.loadSchema(self.groupSchema, inputTuple)
          aggVals = self.loadSchema(self.aggSchema, inputTuple)
          partition = self.groupHashFn(groupVals)


          self.storage.getIndex(partition)
          # # initialize if this is the first we've seen
          # if key not in groupDict: 
          #   groupDict[key] = self.aggExprs[0]

          # curVal = groupDict[key]
          # groupDict[key] = self.aggExprs[1](curVal, aggVals)

          outputTuple = self.aggSchema.instantiate(*[self.joinExprEnv[f] for f in   self.aggSchema.fields])

          self.emitOutputTuple(self.joinSchema.pack(outputTuple))
  
    except StopIteration:
      pass

    for val in groupDict:
      val =self.aggExprs[2](tup)

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

    raise NotImplementedError


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"

  def cost(self):
    return self.selectivity() * self.subPlan.cost()

  # This should be computed based on statistics collection.
  # For now, we simply return a constant.
  def selectivity(self):
    return 1.0
