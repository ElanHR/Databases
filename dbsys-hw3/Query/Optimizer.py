import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Query.Operators.TableScan import TableScan
from Query.Plan import PlanBuilder
from Utils import ExpressionInfo

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass
  ### SELECT * FROM employee JOIN department ON id = eid
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    raise NotImplementedError


  def processJoinOperator(self, relationIds, operator):
    exprInfo = ExpressionInfo.ExpressionInfo(operator.joinExpr)
    relationsInvolved = set()
    for attribute in exprInfo.getAttributes():
      for relId in relationIds:
        # print('Searching ', self.db.relationSchema(relId).fields)
        if attribute in self.db.relationSchema(relId).fields:
          # print('Found ', attribute, ' in ', relId)
          relationsInvolved.add(relId)
          break
    # print(exprInfo.getAttributes())
    return frozenset(relationsInvolved)

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):

    # print(plan.flatten())
    # print(plan.explain())

    optPlan = dict()   # (set of relations) -> best plan to join these relations
    todo = list() # queue of (sets of relations) to be processed
    relationIds = set(plan.relations())

    # add all relations
    for r in relationIds:
      r_set = frozenset({r})
      todo.append(r_set)
      optPlan[r_set] = PlanBuilder(
                          operator=TableScan(
                                r, 
                                self.db.relationSchema(r)), 
                          db=self.db).finalize()

    # add all join expressions
    joinsExps = dict()
    for (_, operator) in plan.flatten():
      if isinstance(operator, Join):
        relationsInvolved  = self.processJoinOperator(relationIds,operator)
        # print(relationsInvolved)
        for r in relationsInvolved:
          # print('adding: ',frozenset({r}))
          joinsExps[frozenset({r})] = (relationsInvolved, operator)



    print('Relations: ', relationIds)

    while len(todo) != 0:
      base = todo.pop(0)
      print('Base=',base)
      if len(base) == len(relationIds): #we've joined all the relations
        return optPlan[base]

      # check all possibilities
      for t in [frozenset({t}) for t in relationIds - base]:

        # if 
        print('t=',t)
        S = frozenset(base.union(t))
        print('S=',S)
        
        je = joinsExps[t]
        if (je==None) or not (je[0].issubset(S)):
          print('Invalid Join')
          continue

        curPlan = Join(optPlan[base],optPlan[t], 
                    expr=je[1].joinExpr,
                    method='block-nested-loops')#je[1].joinMethod)

        print('trying : ', base, ' and ', t)

        if S not in optPlan:
          optPlan[S] = curPlan
        else:
          if curPlan.cost(estimated=True) < optPlan[S].cost(estimated=True):
            optPlan[S] = curPlan

    assert(False)


  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":

  print('hello!')
  import doctest
  doctest.testmod(verbose=True)
