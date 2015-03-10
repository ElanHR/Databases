from Query.Operator import Operator

class TableScan(Operator):
  
  def __init__(self, relId, schema, **kwargs):
    if relId and schema:
      super().__init__(**kwargs)
      self.relId     = relId
      self.relSchema = schema
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
    return None

  # Volcano-style iterator abstraction
  def __iter__(self):
    self.pageIterator = self.storage.pages(self.relId)
    self.nextPageId = None
    self.nextPage   = None
    return self

  # Table scans are always pipelined.
  # While this implementation is more verbose than necessary, it conveys
  # the page-oriented processing style of all operators.
  def __next__(self):
    while not(self.isOutputPageReady()):
      # Table scans in the storage engine return a pair of pageId and page
      pageId, page = next(self.pageIterator)
      self.processInputPage(pageId, page)
    return self.outputPage()

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

  # TODO: return cardinality estimate
  def cost(self):
    return 1000.0

  def selectivity(self):
    return 1.0




