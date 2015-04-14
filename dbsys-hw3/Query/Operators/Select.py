from Query.Operator import Operator

class Select(Operator):
  def __init__(self, subPlan, selectExpr, **kwargs):
    super().__init__(**kwargs)
    self.subPlan    = subPlan
    self.selectExpr = selectExpr

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Select"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for selection operator.

  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.inputFinished = False

    if not self.pipelined:
      self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    if self.pipelined:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.inputFinished = True

      return self.outputPage()

    else:
      return next(self.outputIterator)


  # Page processing and control methods

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    schema = self.subPlan.schema()
    if set(locals().keys()).isdisjoint(set(schema.fields)):
      for inputTuple in page:
        # Load tuple fields into the select expression context
        selectExprEnv = self.loadSchema(schema, inputTuple)

        # Execute the predicate.
        if eval(self.selectExpr, globals(), selectExprEnv):
          self.emitOutputTuple(inputTuple)
    else:
      raise ValueError("Overlapping variables detected with operator schema")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)

    # Process all pages from the child operator.
    try:
      for (pageId, page) in self.inputIterator:
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


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(predicate='" + str(self.selectExpr) + "')"
