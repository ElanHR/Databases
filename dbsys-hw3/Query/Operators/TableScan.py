import random
from Query.Operator import Operator

class TableScan(Operator):

  def __init__(self, relId, schema, **kwargs):
    if relId and schema:
      super().__init__(**kwargs)
      self.relId      = relId
      self.relSchema  = schema
    else:
      raise ValueError("Invalid relation name or schema for a table scan")

  # Returns the output schema of this operator
  def schema(self):
    return self.relSchema

  # Returns the relation accessed by this scan operator,
  # overriding the method in the parent class.
  def relationId(self):
    return self.relId

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return None

  # Returns a string describing the operator type
  def operatorType(self):
    return "TableScan"

  # Returns child operators if present
  def inputs(self):
    return []

  # Volcano-style iterator abstraction
  def __iter__(self):
    self.pageIterator = self.storage.pages(self.relId)
    self.nextPageId, self.nextPage = None, None
    self.pageSize, self.numPages, _ = self.storage.relationStats(self.relId)

    p = max(1, self.cardinality(False) / (self.pageSize * self.sampleFactor))
    self.sampleSize = p if self.sampled else 0
    self.prevPageIndex, self.pagesToSample = (0, p)
    return self

  # Table scans are always pipelined.
  # While this implementation is more verbose than necessary, it conveys
  # the page-oriented processing style of all operators.
  def __next__(self):
    if self.sampled:
      return self.sampledOutput()
    else:
      return self.nextOutput()

  # Returns the next output page for the scan.
  def nextOutput(self):
    while not(self.isOutputPageReady()):
      # Table scans in the storage engine return a pair of pageId and page
      pageId, page = next(self.pageIterator)
      self.processInputPage(pageId, page)
    return self.outputPage()

  # Performs page-based sampling given the scan's sample factor,
  # returning the next output page.
  # Our sampling algorithm returns whole pages, thus the number of
  # sampled elements is ceil(numSamples/pagesize) * pagesize
  #
  # TODO: this uses the page iterator, which still pulls in all
  # pages to the buffer pool. We should directly skip over unused pages.
  def sampledOutput(self):
    if self.pagesToSample > 0:
      pageId, page = None, None
      while pageId is None and page is None:
        pr = self.pagesToSample / (1 + self.numPages - self.prevPageIndex)
        pageId, page = self.nextOutput()
        self.prevPageIndex = pageId.pageIndex
        if pr >= random.random():
          pageId, page = None, None
      self.pagesToSample -= 1
      return (pageId, page)
    else:
      raise StopIteration

  # Returns whether this operator has an output page ready for its iterator.
  def isOutputPageReady(self):
    return not(self.nextPageId is None or self.nextPage is None)

  # Returns the next output page for this operator's iterator.
  def outputPage(self):
    result, self.nextPageId, self.nextPage = (self.nextPageId, self.nextPage), None, None
    return result

  # Table scans simply pass along the next page.
  def processInputPage(self, pageId, page):
    self.nextPageId = pageId
    self.nextPage   = page

  # Table scans do not need this method since they do not produce any new output.
  def emitOutputTuple(self, tupleData):
    raise ValueError("Invalid use of emitOutputTuple in a table scan")

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(" + self.relId + ")"

  # Returns the table's cardinality by using the storage engine.
  def cardinality(self, estimated):
    _, _, r = self.storage.relationStats(self.relId)
    return r

  # Returns the table's cost as the product of the table cardinality,
  # and the per-tuple cost.
  def cost(self, estimated):
    return self.cardinality(estimated) * self.tupleCost

  # A table scan returns a constant selectivity.
  def selectivity(self, estimated):
    return 1.0
