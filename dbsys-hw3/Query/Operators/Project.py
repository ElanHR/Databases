from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Project(Operator):
  """
  A projection operator implementation.

  This requires projection expressions as its parameters.
  Projection expressions are a dictionary of:
    output attribute => expression, attribute type

  For example:
    { 'xplus2'   : ('x+2', 'double'),
      'distance' : ('math.sqrt(x*x+y*y)', 'double') }
  """
  def __init__(self, subPlan, projectExprs, **kwargs):
    super().__init__(**kwargs)
    self.subPlan      = subPlan
    self.projectExprs = projectExprs
    self.outputSchema = DBSchema(self.relationId(), \
                          [(k, v[1]) for (k,v) in self.projectExprs.items()])

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Project"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for projection operator.

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


  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    inputSchema  = self.subPlan.schema()
    outputSchema = self.schema()

    if set(locals().keys()).isdisjoint(set(inputSchema.fields)):
      for inputTuple in page:
        # Execute the projection expressions.
        projectExprEnv = self.loadSchema(inputSchema, inputTuple)
        vals = {k : eval(v[0], globals(), projectExprEnv) for (k,v) in self.projectExprs.items()}
        outputTuple = outputSchema.pack([vals[i] for i in outputSchema.fields])
        self.emitOutputTuple(outputTuple)

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
    return super().explain() + "(projections=" + str(self.projectExprs) + ")"
