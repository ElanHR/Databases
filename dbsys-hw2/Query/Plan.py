import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.Union     import Union
from Query.Operators.Join      import Join
from Query.Operators.GroupBy   import GroupBy
from Query.Operators.Sort      import Sort

class Plan:
  """
  A data structure implementing query plans.
  
  Query plans are tree data structures whose nodes are objects
  inheriting from the Query.Operator class.

  Our Query.Plan class tracks the root of the plan tree,
  and provides basic accessors such as the ability to
  retrieve the relations accessed by the query, the query's
  output schema, and plan pretty printing facilities.

  Plan instances delegate their iterator to the root operator,
  enabling direct iteration over query results.
  
  Plan instances should use the 'prepare' method prior to
  iteration (as done with Database.processQuery), to initialize
  all operators contained in the plan.
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    elif "root" in kwargs:
      self.root = kwargs["root"]

    else:
      raise ValueError("No root operator specified for query plan")

  def fromOther(self):
    self.root = other.root

  # Returns the root operator in the query plan
  def root(self):
    return self.root

  # Returns the query result schema.
  def schema(self):
    return self.root.schema()

  # Returns the relations used by the query.
  def relations(self):
    return [op.relationId() for (_,op) in self.flatten() if isinstance(op, TableScan)]

  # Pre-order depth-first flattening of the query tree.
  def flatten(self):
    if self.root:
      result = []
      queue  = deque([(0, self.root)])
      
      while queue:
        (depth, operator) = queue.popleft()
        children = operator.inputs()
        result.append((depth, operator))
        if children:
          queue.extendleft([(depth+1, c) for c in children])
      
      return result


  # Plan preparation and execution

  # Returns a prepared plan, where every operator has filled in
  # internal parameters necessary for processing data.
  def prepare(self, database):
    if self.root:
      for (_, operator) in self.flatten():
        operator.prepare(database)
      return self
    else:
      raise ValueError("Invalid query plan")

  # Iterator abstraction for query processing.
  # Thus, we can use: "for page in plan: ..."
  def __iter__(self):
    return iter(self.root)

  # Plan and statistics information.

  # Returns a description for the entire query plan, based on the
  # description of each individual operator.
  def explain(self):
    if self.root:
      planDesc = []
      indent = ' ' * 2
      for (depth, operator) in self.flatten():
        planDesc.append(indent * depth + operator.explain())
      
      return '\n'.join(planDesc)

  # Returns the cost of this query plan. Each operator should determine
  # its own local cost added to the cost of its children.
  def cost(self):
    return self.root.cost()

  # Plan I/O, e.g., for query shipping.
  def pack(self):
    raise NotImplementedError

  def unpack(self):
    raise NotImplementedError


class PlanBuilder:
  """
  A query plan builder class that can be used for LINQ-like construction of queries.
  
  A plan builder consists of an operator field, as the running root of the query tree.
  Each method returns a plan builder instance, that can be used to further 
  operators compose with additional builder methods.

  A plan builder yields a Query.Plan instance through its finalize() method.

  >>> import Database
  >>> db = Database.Database()
  >>> db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  >>> schema = db.relationSchema('employee')

  >>> keySchema  = DBSchema('employeeKey',  [('id', 'int')])
  >>> indexId = db.storageEngine().fileMgr.createIndex(schema.name, schema, keySchema, True)

  >>> db.storageEngine().fileMgr.hasIndex(schema.name, keySchema)
  True

  # Populate relation
  >>> tupId = []
  >>> insertedTuples = [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(20)]
  >>> for tup in insertedTuples:
  ...    tupId.append(db.insertTuple(schema.name, tup))
  ...

  ### SELECT * FROM Employee WHERE age < 30
  >>> query1 = db.query().fromTable('employee').where("age < 30").finalize()
  
  >>> query1.relations()
  ['employee']

  >>> print(query1.explain()) # doctest: +ELLIPSIS
  Select[...,cost=...](predicate='age < 30')
    TableScan[...,cost=...](employee)

  >>> [schema.unpack(tup).age for page in db.processQuery(query1) for tup in page[1]]
  [20, 22, 24, 26, 28]


  ### SELECT eid FROM Employee WHERE age < 30
  >>> query2 = db.query().fromTable('employee').where("age < 30").select({'id': ('id', 'int')}).finalize()

  >>> print(query2.explain()) # doctest: +ELLIPSIS
  Project[...,cost=...](projections={'id': ('id', 'int')})
    Select[...,cost=...](predicate='age < 30')
      TableScan[...,cost=...](employee)

  >>> [query2.schema().unpack(tup).id for page in db.processQuery(query2) for tup in page[1]]
  [0, 1, 2, 3, 4]


  ### SELECT * FROM Employee UNION ALL Employee
  >>> query3 = db.query().fromTable('employee').union(db.query().fromTable('employee')).finalize()
  
  >>> print(query3.explain()) # doctest: +ELLIPSIS
  UnionAll[...,cost=...]
    TableScan[...,cost=...](employee)
    TableScan[...,cost=...](employee)

  >>> [query3.schema().unpack(tup).id for page in db.processQuery(query3) for tup in page[1]] # doctest:+ELLIPSIS
  [0, 1, 2, ..., 19, 0, 1, 2, ..., 19]

  ### SELECT * FROM Employee E1 JOIN Employee E2 ON E1.id = E2.id
  >>> e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2'})
  
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('employee'), \
        rhsSchema=e2schema, \
        method='block-nested-loops', expr='id == id2').finalize()
  
  >>> print(query4.explain()) # doctest: +ELLIPSIS
  BNLJoin[...,cost=...](expr='id == id2')
    TableScan[...,cost=...](employee)
    TableScan[...,cost=...](employee)

  >>> q4results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]
  >>> [(tup.id, tup.id2) for tup in q4results] # doctest:+ELLIPSIS
  [(0, 0), (1, 1), (2, 2), ..., (18, 18), (19, 19)]

  ### Hash join test with the same query.
  ### SELECT * FROM Employee E1 JOIN Employee E2 ON E1.id = E2.id
  >>> e2schema   = schema.rename('employee2', {'id':'id2', 'age':'age2'})

  >>> keySchema2 = DBSchema('employeeKey2', [('id2', 'int')])
  
  >>> query5 = db.query().fromTable('employee').join( \
          db.query().fromTable('employee'), \
          rhsSchema=e2schema, \
          method='hash', \
          lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2, \
        ).finalize()
  
  >>> print(query5.explain()) # doctest: +ELLIPSIS
  HashJoin[...,cost=...](lhsKeySchema=employeeKey[(id,int)],rhsKeySchema=employeeKey2[(id2,int)],lhsHashFn='hash(id) % 4',rhsHashFn='hash(id2) % 4')
    TableScan[...,cost=...](employee)
    TableScan[...,cost=...](employee)

  >>> q5results = [query5.schema().unpack(tup) for page in db.processQuery(query5) for tup in page[1]]
  >>> sorted([(tup.id, tup.id2) for tup in q5results]) # doctest:+ELLIPSIS
  [(0, 0), (1, 1), (2, 2), ..., (18, 18), (19, 19)]

  ### Index join test with the same query.
  >>> db.storageEngine().fileMgr.hasIndex(schema.name, keySchema)
  True
  
  >>> indexId
  1
  >>> 
  
  ### SELECT * FROM Employee E1 JOIN Employee E2 ON E1.id = E2.id
  
  >>> query8 = db.query().fromTable('employee').join( \
          db.query().fromTable('employee'), \
          rhsSchema=e2schema, \
          method='indexed',lhsKeySchema=keySchema, indexId=indexId).finalize()
  
  >>> print(query8.explain()) # doctest: +ELLIPSIS
  IndexJoin[16,cost=1000000.00](indexKeySchema=employeeKey[(id,int)])
    TableScan[15,cost=1000.00](employee)
    TableScan[14,cost=1000.00](employee)

  >>> q8results = [query8.schema().unpack(tup) for page in db.processQuery(query8) for tup in page[1]]
  >>> sorted([(tup.id, tup.id2) for tup in q8results]) # doctest:+ELLIPSIS
  [(0, 0), (1, 1), (2, 2), ..., (18, 18), (19, 19)]

  
  ### Group by aggregate query
  ### SELECT id, max(age) FROM Employee GROUP BY id
  >>> aggMinMaxSchema = DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])
  >>> query6 = db.query().fromTable('employee').groupBy( \
          groupSchema=keySchema, \
          aggSchema=aggMinMaxSchema, \
          groupExpr=(lambda e: e.id), \
          aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), \
                    (0, lambda acc, e: max(acc, e.age), lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
        ).finalize()
  
  >>> print(query6.explain()) # doctest: +ELLIPSIS
  GroupBy[...,cost=...](groupSchema=employeeKey[(id,int)], aggSchema=minmax[(minAge,int),(maxAge,int)])
    TableScan[...,cost=...](employee)

  >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  >>> sorted([(tup.id, tup.minAge, tup.maxAge) for tup in q6results]) # doctest:+ELLIPSIS
  [(0, 20, 20), (1, 22, 22), ..., (18, 56, 56), (19, 58, 58)]

  ### Order by query
  ### SELECT id FROM Employee ORDER by age
  >>> query7 = db.query().fromTable('employee') \
        .order(sortKeyFn=lambda x: x.age, sortKeyDesc='age') \
        .select({'id': ('id', 'int')}).finalize()

  >>> print(query7.explain()) # doctest: +ELLIPSIS
  Project[...,cost=...](projections={'id': ('id', 'int')})
    Sort[...,cost=...](sortKeyDesc='age')
      TableScan[...,cost=...](employee)
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    elif "operator" in kwargs:
      self.operator = kwargs["operator"]

    elif "db" in kwargs:
      self.database = kwargs["db"]

    else:
      raise ValueError("No initial operator or database given for a plan builder")

  def fromOther(self, other):
    self.database = other.database
    self.operator = other.operator

  def fromTable(self, relId):
    if self.database:
      schema = self.database.relationSchema(relId)
      return PlanBuilder(operator=TableScan(relId, schema))

  def where(self, conditionExpr):
    if self.operator:
      return PlanBuilder(operator=Select(self.operator, conditionExpr))
    else:
      raise ValueError("Invalid where clause")

  def select(self, projectExprs):
    if self.operator:
      return PlanBuilder(operator=Project(self.operator, projectExprs))
    else:
      raise ValueError("Invalid select list")

  def join(self, rhsQuery, **kwargs):
    if rhsQuery:
      rhsPlan = rhsQuery.operator
    else:
      raise ValueError("Invalid Join RHS query")
    
    lhsPlan = self.operator
    return PlanBuilder(operator=Join(lhsPlan, rhsPlan, **kwargs))

  def union(self, subQuery):
    if self.operator:
      return PlanBuilder(operator=Union(self.operator, subQuery.operator))
    else:
      raise ValueError("Invalid union clause")

  def groupBy(self, **kwargs):
    if self.operator:
      return PlanBuilder(operator=GroupBy(self.operator, **kwargs))
    else:
      raise ValueError("Invalid group by operator")

  def order(self, **kwargs):
    if self.operator:
      return PlanBuilder(operator=Sort(self.operator, **kwargs))
    else:
      raise ValueError("Invalid order by operator")

  # Constructs a plan instance from the running plan tree.
  def finalize(self):
    if self.operator:
      return Plan(root=self.operator)
    else:
      raise ValueError("Invalid query plan")


if __name__ == "__main__":
    import doctest
    doctest.testmod()
