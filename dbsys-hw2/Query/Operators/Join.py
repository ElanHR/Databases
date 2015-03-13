import itertools
import sys

from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())
    
    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]
    
    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema] 
    
    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]
    
    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join. 
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None \
          or self.storage.getIndex(self.indexId) is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL' 
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.lhsPlan)
    self.inputFinished = False

    self.outputIterator = self.processAllPages()
    
    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()
    
    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()
    
    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())





  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.
  def blockNestedLoops(self):
    
    pinnedPages = self.accessPageBlock(self.storage.bufferPool, self.lhsPlan)
    #TODO Pin right pages, handle overflow
    result = self.nestedLoops()
    self.removePageBlock(self.storage.bufferPool, pinnedPages)
    return result
      
  
  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    pageIds = []
    for (lPageId, lhsPage) in pageIterator:
      bufPool.pinPage(lPageId)
      pageIds.append(lPageId)
    return pageIds

  def removePageBlock(self, bufPool, pageIds):
    for pageId in pageIds:
      bufPool.unpinPage(pageId)


  ##################################
  #
  # Indexed nested loops implementation
  #
  def indexedNestedLoops(self):
    raise NotImplementedError


  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    #Create lhs partition dictionary
    #Hash values are the key
    #File Ids are the values
    lhs_partitions = {}
    
    #Hash lhs
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        
        #lhs_ExprEnv = self.loadSchema(self.lhsSchema, lTuple)
        lhs_HashEnv = self.loadSchema(self.lhsKeySchema, self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema))
        hashVal = eval(self.lhsHashFn, globals(), lhs_HashEnv)
        
        #If we have seen the hash value before, put it in the associated partition
        if hashVal in lhs_partitions:
          relId = lhs_partitions[hashVal]
          tupId = self.storage.insertTuple(relId, lTuple)
          self.storage.fileMgr.relationFile(relId)[1].tuples

        else:
          #We have not encountered this partition, create it
          relId = "lhs_" + str(hashVal)
          newFile = self.createPartition(relId, self.lhsSchema)
          lhs_partitions[hashVal] = relId
          self.storage.insertTuple(relId, lTuple)


    #Create rhs partitions
    rhs_partitions = {}

    #Hash rhs
    for (rPageId, rhsPage) in iter(self.rhsPlan):
      for rTuple in rhsPage:
        rhs_HashEnv = self.loadSchema(self.rhsKeySchema, self.rhsSchema.projectBinary(rTuple, self.rhsKeySchema))

        hashVal = eval(self.rhsHashFn, rhs_HashEnv)
        
        if hashVal in rhs_partitions:
          relId = rhs_partitions[hashVal]
          self.storage.insertTuple(relId, rTuple)

        else:
          relId = "rhs_" + str(hashVal)
          newFile = self.createPartition(relId, self.rhsSchema)
          rhs_partitions[hashVal] = relId
          tupId = self.storage.insertTuple(relId, rTuple)

      outputId = "hashOut"
      output = self.storage.createRelation(outputId, self.joinSchema)

      for key in lhs_partitions:
        lId = lhs_partitions[key]
        if key in rhs_partitions:
          rId = rhs_partitions[key]

          
          rhs_file = self.storage.fileMgr.relationFile(rId)[1]
          lhs_file = self.storage.fileMgr.relationFile(lId)[1]

          self.block_nested_loop_locals(outputId, self.rhsSchema, self.lhsSchema, rhs_file, lhs_file)
          self.storage.removeRelation(rId)
        self.storage.removeRelation(lId)
        
    
    
    return self.storage.pages(outputId)


  def createPartition(self, relId, keySchema):

    #relId = self.relationId()
    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)
    
    self.storage.createRelation(relId, keySchema)

  
  
  def block_nested_loop_locals(self, output, rSchema, lSchema, rFile, lFile):


    
    pinnedlhsPages = self.accessPageBlock(self.storage.bufferPool, lFile.pages())
    pinnedrhsPages = self.accessPageBlock(self.storage.bufferPool, rFile.pages())

  
    for (lPageId, lhsPage) in lFile.pages():
      for lTuple in lhsPage:
        
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(lSchema, lTuple)

        for (rPageId, rhsPage) in rFile.pages():
          for rTuple in rhsPage:           
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(rSchema, rTuple))
            
            if self.joinExpr:
              # Evaluate the join predicate, and output if we have a match.
              if eval(self.joinExpr, globals(), joinExprEnv):
                outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                self.storage.insertTuple(output, self.joinSchema.pack(outputTuple))
            else:
              lhs_HashEnv = self.loadSchema(self.lhsKeySchema, self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema))
              rhs_HashEnv = self.loadSchema(self.rhsKeySchema, self.rhsSchema.projectBinary(rTuple, self.rhsKeySchema))
              if lTuple == rTuple:
                outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                self.storage.insertTuple(output, self.joinSchema.pack(outputTuple))
              
  
    self.removePageBlock(self.storage.bufferPool, pinnedlhsPages)
    self.removePageBlock(self.storage.bufferPool, pinnedrhsPages)


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"
    
    return super().explain() + exprs

  # Join costs must consider the cost of matching all pairwise inputs.
  def cost(self):
    numMatches = self.lhsPlan.cost() * self.rhsPlan.cost()
    return self.selectivity() * numMatches

  # This should be computed based on statistics collection.
  # For now, we simply return a constant.
  def selectivity(self):
    return 1.0

