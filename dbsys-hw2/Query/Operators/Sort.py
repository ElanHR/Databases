from Catalog.Schema import DBSchema
from Query.Operator import Operator

import operator, sys 


# Operator for External Sort
class Sort(Operator):

  '''

    ### Order by query
    ### SELECT id FROM Employee ORDER by age
    >>> query7 = db.query().fromTable('employee') \
          .order(sortKeyFn=lambda x: x.age, sortKeyDesc='age') \
          .select({'id': ('id', 'int')}).finalize()

    >>> print(query7.explain()) # doctest: +ELLIPSIS
    Project[...,cost=...](projections={'id': ('id', 'int')})
      Sort[...,cost=...](sortKeyDesc='age')
        TableScan[...,cost=...](employee)

  '''


  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    self.sortedFiles = []

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.outputIterator = self.processAllPages()
    return self.outputIterator

  def __next__(self):
    return next(self.outputIterator)


  # Page processing and control methods

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    # load all tuples into memory
    tuples = [self.schema().unpack(e) for e in page]

    # sort in memory
    sorted(tuples, key=self.sortKeyFn) 

    # make a temp page for sorted results
    relId = 's'+str(pageId.fileId.fileIndex)+'-'+str(pageId.pageIndex)
    self.storage.createRelation( relId, self.schema())
    assert(self.storage.hasRelation(relId))

    # push sorted tuples into page
    for tup in tuples:
      self.storage.insertTuple(relId, self.schema().pack(tup))

    self.sortedFiles.append(relId)


  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)

    # sort all input pages
    for (pageId, page) in self.inputIterator: 
      self.processInputPage(pageId,page)

    # N-way merge those pages

    #initialize
    sorted_iterators = []
    lowest = []
    done = []

    # print('========================')
    for relID in self.sortedFiles:
      # print('relID: ',relID)

      it = self.storage.tuples(relID) 
      # print('it:',[e for e in it])
      try: #if it.has_next():
        sorted_iterators.append( self.storage.tuples(relID) )
        lowest.append(  self.schema().unpack(next(it)))
        done.append(False)
      except StopIteration:
        pass

    # iterators that still need to be processed
    # to_be_processed = [ self.schema().unpack(it) for (idx,it) in enumerate(sorted_iterators) if !done(idx) ] 
    # print('sortedFiles: ',self.sortedFiles)
    # print('lowest: ', lowest)
    # print('done: ', done)
    sys.stdout.flush()

    while all(d != True for d in done):
      min_idx, outputTuple = min( enumerate(lowest), key=lambda x: self.sortKeyFn(x[1])  )

      try:
        lowest[min_idx] = self.schema().unpack(next(sorted_iterators[min_idx]))
      except:
        lowest[min_idx] = sys.maxsize# remove the index
        done[min_idx] = True

      # print(outputTuple)
      self.emitOutputTuple(self.schema().pack(outputTuple))
    
    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"

  def cost(self):
    return self.selectivity() * self.subPlan.cost()

  def selectivity(self):
    return 1.0

