import itertools
import sys

from collections import deque

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Query.Operators.Union import Union
from Utils.ExpressionInfo import ExpressionInfo
from Query.Operators.TableScan import TableScan
from Query.Plan import PlanBuilder


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

  >>> pushDown = db.optimizer.pushdownOperators(query5)
  >>> type(pushDown) is Plan
  True
  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    self.statsCache[plan.relations()] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    return self.statsCache[plan.relations()]

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):

    root = plan.root
    newRoot = None

    result = []
    
    #Stack in the form of CurrentOp, ParentOp, SelectToAdd, ChildType(Left/Right/Only)
    queue = deque([(root, None, None, None, "None")])

    while queue:

      (op, parent, selectAdd, projectAdd, childType)  = queue.popleft()
      children = op.inputs()

      #At a join, groupby, select, project, or union
      if children:
        if op.operatorType() == "Select":
          child = children[0]
          self.select(result, (op, parent, selectAdd, projectAdd, childType), child, queue)
          
        elif op.operatorType() == "Project":
          child = children[0] 
          self.project(result, (op, parent, selectAdd, projectAdd, childType), child, queue)
                             
        elif op.operatorType == "GroupBy":
          self.groupBy(result, (op, parent, selectAdd, projectAdd, childType), queue)

        elif op.operatorType == "Join":
          self.join(result, (op, parent, selectAdd, projectAdd, childType), queue)
                             
        else:
          self.union(result, (op, parent, selectAdd, projectAdd, childType), queue)

      #At table scan
      else:
        self.tableScan(result, (op, parent, selectAdd, projectAdd, childType))

          

    tree = self.arrayToTree(result)

    self.flatPrint(tree)

    plan = Plan(root=tree)
    
    return plan

  def arrayToTree(self, result):
    tempArray = []
    
    for op, parent in reversed(result):
      if op.operatorType() == "TableScan":
        tempArray.append(op)
      elif op.operatorType() == "Project":
        project = Project(tempArray.pop(), op.projectExprs)
        tempArray.append(project)
      elif op.operatorType() == "Select":
        select = Select(tempArray.pop(), op.selectExpr)
        tempArray.append(select)
      elif "Join" in op.operatorType():
        op.lhsPlan = tempArray.pop()
        op.rhsPlan = tempArray.pop()
        tempArray.append(op)
      elif op.operatorType() == "UnionAll":
        lhs = tempArray.pop()
        rhs = tempArray.pop()
        union = Union(lhs, rhs)
        sys.stderr.write(union.explain() + "\n")
        sys.stderr.flush()
        tempArray.append(union)
      elif op.operatorType() == "GroupBy":
        op.subPlan = tempArray.pop()
        tempArray.append(op)
    return tempArray.pop()

  # Pre-order depth-first flattening of the query tree.
  def flatPrint(self, root):
    result = []
    queue  = deque([(0, root)])

    while queue:
      (depth, operator) = queue.popleft()
      children = operator.inputs()
      result.append((depth, operator))
      if children:
        queue.extendleft([(depth+1, c) for c in children])

    for depth, f in result:
      sys.stderr.write(str(depth) + " " + f.explain() + "\n")
      sys.stderr.flush()
        
  #Pushdown helper for GroupBy
  #Don't need to push through groupBys, so place all selects and projects
  def groupBy(self, result, queueEntry, queue):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry
    
    newSelect = None
    newProject = None

    #Place all selects
    if selectAdd:
      expr = ''
      for e in selectAdd:
        if len(expr) > 0:
          expr += 'and'
        expr += e
      newSelect = Select(op, expr)
      result.append((newSelect, parent))

    #Place all projects
    if projectAdd:
      newProject = Project(op, projectAdd)
      if newSelect:
        result.append((newProject, newSelect))
      else:
        result.append((newProject, parent))

    #Place GroupBy
    if newProject:
      result.append((op, newProject))
    elif newSelect:
      result.append((op, newSelect))
    else:
      result.append((op, parent))

    child = children[0]
    queue.extendleft((child, op, None, None, "Only"))
    
  #Pushdown helper for project
  def project(self, result, queueEntry, child, queue):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry

    if child.operatorType() == "Select":
      selectAttributes = ExpressionInfo(child.selectExpr).getAttributes()
      projectAttributes = self.getProjectAttributes(op.projectExprs)

      if set(selectAttributes).issubset(set(projectAttributes)):
        if projectAdd:
          projectAdd.update(op.projectExprs)
        else:
          projectAdd = op.projectExprs
          queue.extendleft([(child, parent, selectAdd, projectAdd, "Only")])
      else:
        result.append((op, parent))
        queue.extendleft([(child, op, selectAdd, projectAdd, "Only")])
  

    else:
      if projectAdd:
        projectAdd.update(op.projectExprs)
      queue.extendleft([(child, op, selectAdd, projectAdd, "Only")])

  #Helper for project pushdown.  Gets attributes involved with project
  def getProjectAttributes(self, projectExprs):
    attributes = set()
    for key, value in projectExprs.items():
      attrib = ExpressionInfo(value[0]).getAttributes()
      for a in attrib:
        attributes.add(a)

    return attributes
        
  #Pushdown helper for select
  def select(self, result, queueEntry, child, queue):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry
    if not selectAdd:
      selectAdd = []
    for e in ExpressionInfo(op.selectExpr).decomposeCNF():
      selectAdd.append(e)
    queue.extendleft([(child, op, selectAdd, projectAdd, "Only")])
    
  #Leaf of the plan tree, no pushdown through this
  def tableScan(self, result, queueEntry):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry
    newSelect = None
    newProject = None
    if selectAdd:
      expr = ''
      for e in selectAdd:
        if len(expr) > 0:
          expr += 'and'
        expr += e
      newSelect = Select(op, expr)
      result.append((newSelect, parent))
    if projectAdd:
      newProject = Project(op, projectAdd)
      if newSelect:
        result.append((newProject, newSelect))
      else:
        result.append((newProject, parent))
    if newProject:
      result.append((op, newProject))
    elif newSelect:
      result.append((op, newSelect))
    else:
      result.append((op, parent))
    
  def join(self, result, queueEntry, queue):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry
    
    newSelect = None
    newProject = None
    
    keep = None
    moveLeft = None
    moveRight = None

    rightPro = None
    leftPro = None
    keepPro = None

    rhsSchema = op.rhsSchema
    lhsSchema = op.lhsSchema

    if selectAdd:
      keep = []
      moveLeft = []
      moveRight = []
      for e in selectAdd:
        attrib = ExpressionInfo(e).getAttributes()
        left = set(attrib) < set(lhsSchema.fields)
        right = set(attrib) < set(rhsSchema.fields)

        #Both sides have all attributes
        if left and right:
          keep.append(e)

        #Neither side has all attributes
        elif left == right:
          keep.append(e)

        #Move select left
        elif left:
          moveLeft.append(e)
            
        #Move select right
        else:
          moveRight.append(e)

        if len(keep) > 0:
          expr = ''
          for e in keep:
            if len(expr) > 0:
              expr += 'and'
          expr += e
          newSelect = Select(op, expr)
          result.append((newSelect, parent))
          
      if projectAdd:

        projectAttrib = self.getProjectAttributes(projectAdd)
        joinAttrib = ExpressionInfo(op.joinExpr).getAttributes()
        #Projection includes Join Expression
        if set(joinAttrib).issubset(set(projectAttrib)):
          (rightPro, leftPro, keepPro) = self.findRHSandLHSProject(projectAdd, rhsSchema.fields, lhsSchema.fields)
        if len(keepPro) > 0:
          newProject = Project(op, keepPro)
          if newSelect:
            result.append((newProject, newSelect))
          else:
            result.append((newProject, parent))

      if newProject:
        result.append((op, newProject))
      elif newSelect:
        result.append((op, newSelect))
      else:
        result.append((op, parent))

      queue.extendleft([(op.lhsPlan, op, moveLeft, leftPro, "Left")])
      queue.extendleft([(op.rhsPlan, op, moveRight, rightPro, "Right")])

  def union(self, result, queueEntry, queue):
    (op, parent, selectAdd, projectAdd, childType) = queueEntry
    
    newSelect = None
    newProject = None
    
    keep = None
    moveLeft = None
    moveRight = None

    rightPro = None
    leftPro = None
    keepPro = None

    rhsSchema = op.rhsPlan.schema()
    lhsSchema = op.lhsPlan.schema()
      
    if selectAdd:
      keep = []
      moveLeft = []
      moveRight = []
      for e in selectAdd:
        attrib = ExpressionInfo(e).getAttributes()
        left = set(attrib) < set(lhsSchema.fields)
        right = set(attrib) < set(rhsSchema.fields)

        #Both sides have all attributes
        if left and right:
          moveLeft.append(e)
          moveRight.append(e)

        #Neither side has all attributes
        elif left == right:
          keep.append(e)

        #Move select left
        elif left:
          moveLeft.append(e)
            
        #Move select right
        else:
          moveRight.append(e)

        if len(keep) > 0:
          expr = ''
          for e in keep:
            if len(expr) > 0:
              expr += 'and'
          expr += e
          newSelect = Select(op, expr)
          result.append((newSelect, parent))
          
      if projectAdd:

        projectAttrib = self.getProjectAttributes(projectAdd)
        (rightPro, leftPro, keepPro) = self.findRHSandLHSProject(projectAdd, rhsSchema.fields, lhsSchema.fields)
        if len(keepPro) > 0:
          newProject = Project(op, keepPro)
          if newSelect:
            result.append((newProject, newSelect))
          else:
            result.append((newProject, parent))

      if newProject:
        result.append((op, newProject))
      elif newSelect:
        result.append((op, newSelect))
      else:
        result.append((op, parent))

      queue.extendleft([(op.lhsPlan, op, moveLeft, leftPro, "Left")])
      queue.extendleft([(op.rhsPlan, op, moveRight, rightPro, "Right")])

  def findRHSandLHSProject(self, projectExpr, rhs, lhs):
    right = {}
    left = {}
    keep = {}

    for key, value in projectExpr.items():
      isRight = False
      isLeft = False
      attrib = ExpressionInfo(value[0]).getAttributes()
      if set(attrib).issubset(set(rhs)):
        isRight = True
        right[key] = value
      if set(attrib).issubset(set(lhs)):
        isLeft = True
        left[key] = value
      if not isRight and not isLeft:
        keep[key] = value

    return right, left, keep

  def processJoinOperator(self, relationIds, operator):
    exprInfo = ExpressionInfo(operator.joinExpr)
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
          self.addPlanCost(curPlan, curPlan.cost(estimated=False))
        else:
          curCost = curPlan.cost(estimated=False)
          if curCost < self.getPlanCost(optPlan[S]):
            optPlan[S] = curPlan
            self.addPlanCost(curPlan, curCost)


    assert(False)


  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":

  print('Optimizer __main__')
  import doctest
  doctest.testmod(verbose=True)
