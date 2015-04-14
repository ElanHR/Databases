from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Union(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)
    self.lhsPlan  = lhsPlan
    self.rhsPlan  = rhsPlan
    self.validateSchema()

  # Unions must have equivalent schemas on their left and right inputs.
  def validateSchema(self):
    if self.lhsPlan.schema().match(self.rhsPlan.schema()):
      schemaName       = self.operatorType() + str(self.id())
      schemaFields     = self.lhsPlan.schema().schema()
      self.unionSchema = DBSchema(schemaName, schemaFields)
    else:
      raise ValueError("Union operator type error, mismatched input schemas")

  def schema(self):
    return self.unionSchema

  def inputSchemas(self):
    return [self.lhsPlan.schema(), self.rhsPlan.schema()]

  def operatorType(self):
    return "UnionAll"

  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for union operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputFinished = False
    self.inputIterators = list(zip(map(lambda x: iter(x), self.inputs()), self.inputSchemas()))

    self.currentInputIterator = self.inputIterators[0][0]
    self.currentSchema        = self.inputIterators[0][1]

    if not self.pipelined:
      self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    if self.pipelined:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.currentInputIterator)
          self.processInputPage(pageId, page)

        except StopIteration:
          self.inputIterators.pop(0)
          if self.inputIterators:
            self.currentInputIterator = self.inputIterators[0][0]
            self.currentSchema        = self.inputIterators[0][1]
          else:
            self.inputFinished = True

      return self.outputPage()

    else:
      return next(self.outputIterator)

  # Page processing and control methods

  # Page-at-a-time operator processing
  # For union all, this copies over the input tuple to the output
  def processInputPage(self, pageId, page):
    for inputTuple in page:
      self.emitOutputTuple(inputTuple)

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterators is None:
      self.inputIterators = list(zip(map(lambda x: iter(x), self.inputs()), self.inputSchemas()))

    # Process all pages from the child operator.
    for (currentInputIterator, currentSchema) in self.inputIterators:
      try:
        for (pageId, page) in currentInputIterator:
          self.processInputPage(pageId, page)

          # No need to track anything but the last output page when in batch mode.
          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]

      # To support pipelined operation, processInputPage may raise a
      # StopIteration exception during its work. We catch this and ignore in batch mode.
      except StopIteration:
        pass

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())
