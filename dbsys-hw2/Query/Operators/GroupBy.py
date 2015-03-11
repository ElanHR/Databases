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

  # Iterator abstraction for groupBy operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.inputFinished = False

    self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    return next(self.outputIterator)


  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for group-by")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)

    self.initializeOutput()
    # outputPage = self.outputPages[0]

    groupDict = dict()
    # Process all pages from the child operator.
    try:
      # Loop through all tuples
      for (pageId, page) in self.inputIterator: 
        for inputTuple in page:

          # Partition = hashFn( groupSchema(tuple) )
          # aggregate = lambda( aggreSchema(tuple) )
          # (Partition, Group) = aggragate


          # insertTuple,
          # deleteTuple
          # updateTuple

          # group value
          groupVals = self.subSchema.projectBinary(inputTuple, self.groupSchema)
          # groupVals = self.groupSchema.unpack(groupVals)

          # partition 
          partition = self.groupHashFn(groupVals)
          
          # tuple values involved in aggregate
          # aggVals   = self.subSchema.projectBinary(inputTuple, self.aggSchema)
          curInVal = self.subSchema.unpack(inputTuple)

          key = (partition,groupVals)

          # self.storage.getIndex(partition)

          # initialize if this is the first we've seen this group
          if key not in groupDict:
            # print('',key,' is not in dict! Initializing...')
            curAggVal = self.aggSchema.instantiate( *[ e[0] for e in self.aggExprs ])
          else:
            curAggVal = self.aggSchema.unpack( groupDict[key] )
 
          print('Key: ', self.groupSchema.unpack(key[1]) )
          print('Old Val: ', curAggVal )
          # import pdb
          # pdb.set_trace()


          #for each lambda expression...
          groupDict[key] = self.aggSchema.pack( self.aggSchema.instantiate(\
            *[ self.aggExprs[i][1](curAggVal[i], curInVal) for i in range(len(self.aggExprs))]))

          print('New Val: ', self.aggSchema.unpack(groupDict[key]) )
  
    except StopIteration:
      pass

    print('outputSchema: ', self.outputSchema.toString())
    for k,v in groupDict.items():
      # print('K: ', k, '\tV:',v)
      

      # Run final aggregate lambda
      curVal = self.aggSchema.unpack(v)
      # print('curVal: ',curVal)
      finalVal = self.aggSchema.instantiate(\
            *[ self.aggExprs[i][2](curVal[i]) for i in range(len(self.aggExprs))])
      # print('finalVal: ',finalVal)
      v = self.aggSchema.pack( finalVal )

      # Create output tuples
      output = self.loadSchema(self.groupSchema, k[1])
      output.update(self.loadSchema(self.aggSchema, v))

      outputTuple = self.outputSchema.instantiate(*[output[f] for f in self.outputSchema.fields])

      print(outputTuple)

      self.emitOutputTuple(self.outputSchema.pack(outputTuple))

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


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
