import itertools

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
    self.partitionFiles = {0:{}, 1:{}}
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

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    pageBlock = []
    try:
      while True:
        (pageId, page) = next(pageIterator)
        pageBlock.append((pageId, page))
        bufPool.pinPage(pageId)
        if bufPool.numFreePages() == 0:
          break
    except StopIteration:
      pass

    return pageBlock

  def blockNestedLoops(self):
    # Access the outer relation's block, pinning pages in the buffer pool.
    bufPool    = self.storage.bufferPool
    lhsIter    = iter(self.lhsPlan)
    lPageBlock = self.accessPageBlock(bufPool, lhsIter)

    while lPageBlock:
      for (lPageId, lhsPage) in lPageBlock:
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

        # Unpin the page after joining with the RHS relation.
        # Thus future accesses can evict the page while reading the next block.
        bufPool.unpinPage(lPageId)

      # Move to the next page block after processing it.
      lPageBlock = self.accessPageBlock(bufPool, lhsIter)

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO: test
  def indexedNestedLoops(self):
    if self.indexId:
      bufPool = self.storage.bufPool
      for (lPageId, lhsPage) in iter(self.lhsPlan):
        for lTuple in lhsPage:
          # Load the lhs once per inner loop.
          joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

          # Match against RHS tuples using the index.
          joinKey = self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema)
          matches = self.storage.lookupByIndex(self.indexId, joinKey)

          for rhsTupId in matches:
            rhsPage = bufPool.getPage(rhsTupId.pageId)
            rTuple  = rhsPage.getTuple(rhsTupId)

            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate any remaining join predicate, and output if we have a match.
            fullMatch = eval(self.joinExpr, globals(), joinExprEnv) if self.joinExpr else True
            if fullMatch:
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

          # No need to track anything but the last output page when in batch mode.
          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]

      # Return an iterator to the output relation
      return self.storage.pages(self.relationId())

    else:
      raise ValueError("No index found while using an indexed nested loops join")


  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    # Partition the LHS and RHS inputs, creating a temporary file for each partition.
    # We assume one-level of partitioning is sufficient and skip recurring.
    for (lPageId, lPage) in iter(self.lhsPlan):
      for lTuple in lPage:
        lPartEnv = self.loadSchema(self.lhsSchema, lTuple)
        lPartKey = eval(self.lhsHashFn, globals(), lPartEnv)
        self.emitPartitionTuple(lPartKey, lTuple, left=True)

    for (rPageId, rPage) in iter(self.rhsPlan):
      for rTuple in rPage:
        rPartEnv = self.loadSchema(self.rhsSchema, rTuple)
        rPartKey = eval(self.rhsHashFn, globals(), rPartEnv)
        self.emitPartitionTuple(rPartKey, rTuple, left=False)

    # Iterate over partition pairs and output matches
    # evaluating the join expression as necessary.
    for ((lPageId, lPage), (rPageId, rPage)) in self.partitionPairs():
      for lTuple in lPage:
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
        for rTuple in rPage:
          joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
          output = \
            ( self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema) \
                == self.rhsSchema.projectBinary(rTuple, self.rhsKeySchema) ) \
            and ( eval(self.joinExpr, globals(), joinExprEnv) if self.joinExpr else True )

          if output:
            outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
            self.emitOutputTuple(self.joinSchema.pack(outputTuple))

      # No need to track anything but the last output page when in batch mode.
      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    # Clean up partitions.
    self.removePartitionFiles()

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  # Hash join helpers.
  def partitionRelationId(self, left, partitionId):
    return self.operatorType() + str(self.id()) + "_" \
            + ("l" if left else "r") + "part_" + str(partitionId)

  def emitPartitionTuple(self, partitionId, partitionTuple, left=False):
    partRelId  = self.partitionRelationId(left, partitionId)
    partSchema = self.lhsSchema if left else self.rhsSchema

    # Create a partition file as needed.
    if not self.storage.hasRelation(partRelId):
      self.storage.createRelation(partRelId, partSchema)
      self.partitionFiles[int(left)][partitionId] = partRelId

    partFile = self.storage.fileMgr.relationFile(partRelId)[1]
    if partFile:
      partFile.insertTuple(partitionTuple)

  # Return pairs of pages from matching partitions.
  def partitionPairs(self):
    lKeys = self.partitionFiles[0].keys()
    rKeys = self.partitionFiles[1].keys()
    matches = [(self.partitionFiles[0][partId], self.partitionFiles[1][partId]) \
                for partId in lKeys if partId in rKeys]
    return PartitionIterator(matches, self.storage)

  # Delete all existing partition files.
  def removePartitionFiles(self):
    for lPartRelId in self.partitionFiles[0].values():
      self.storage.removeRelation(lPartRelId)

    for rPartRelId in self.partitionFiles[1].values():
      self.storage.removeRelation(rPartRelId)

    self.partitionFiles = {0:{}, 1:{}}


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

# An iterator class for looping over pairs of pages from partition files.
class PartitionIterator:
  def __init__(self, partFiles, storageEngine):
    self.partFiles = partFiles
    self.storage   = storageEngine
    self.fileIter  = iter(self.partFiles)
    self.nextFilePair()

  def __iter__(self):
    return self

  def nextFilePair(self):
    try:
      (lPartRelId, rPartRelId) = next(self.fileIter)
      self.lFile = self.storage.fileMgr.relationFile(lPartRelId)[1]
      self.rFile = self.storage.fileMgr.relationFile(rPartRelId)[1]
    except StopIteration:
      self.fileIter     = None
      self.pagePairIter = None
    else:
      self.pagePairIter = itertools.product(self.lFile.pages(), self.rFile.pages())

  def __next__(self):
    if self.fileIter is not None:
      while self.pagePairIter is not None:
        try:
          return next(self.pagePairIter)
        except StopIteration:
          self.nextFilePair()

    if self.fileIter is None:
      raise StopIteration
