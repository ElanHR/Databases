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
from Query.Optimizer import Optimizer

from Query.getKSubsets import k_subsets


class BushyOptimizer(Optimizer):
  
  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    print('BushyJoin')
    print(plan.flatten())
    print('Initial:\n',plan.explain())

    self.totalCombosTried    = 0
    self.totalPlansProcessed = 0

    optPlan = dict()   # (set of relations) -> best plan to join these relations
    relationIds = set(plan.relations())

    # add all relations
    for r in relationIds:
      r_set = frozenset({r})
      new_op = TableScan(r,self.db.relationSchema(r))
      new_op.prepare(self.db)
      optPlan[r_set] = new_op
      
    # add all join expressions
    joinsExps = dict()
    for (_, operator) in plan.flatten():
      if isinstance(operator, Join):
        relationsInvolved  = self.processJoinOperator(relationIds,operator)
        # print('rel involved: ', relationsInvolved)
        for r in [frozenset({r}) for r in relationsInvolved]:

          # print('adding: ',r)
          if r in joinsExps:
            joinsExps[r].append((relationsInvolved, operator))
          else:
            joinsExps[r] = [(relationsInvolved, operator)]



    # print('Relations: ', relationIds)
    # print('Join Exp:  ', joinsExps)
    n = len(relationIds)
    for i in range(2,n+1):
      # for each subset of size i
      for S in [frozenset(S) for S in k_subsets(relationIds,i)]:
        # print('S = ',S)
        # for each subset of S
        for j in range(1,int(i/2)+1):
          for O in [frozenset(O) for O in k_subsets(S,j)]:

            right = frozenset(S-O)
            # print('O = ',O)
            # print('combining',O,right)

            # if there is a relation joining something in Left to something in Right
            for t in O:

              self.totalCombosTried += 1


              je = joinsExps[frozenset({t})]

              if (je==None):
                # print('Invalid Join')
                continue
              else: 
                for join_expression in je:
                  if not join_expression[0].issubset(S) or O not in optPlan or right not in optPlan:
                    # print('Invalid Join')
                    continue

                  self.totalPlansProcessed += 1
                  curPlan = Join(optPlan[O],optPlan[right], 
                        expr=join_expression[1].joinExpr,
                        method='block-nested-loops')
                  curPlan.prepare(self.db)
                  
                  if S not in optPlan:
                    optPlan[S] = curPlan
                    self.addPlanCost(S, curPlan.cost(estimated=True))
                    # print('Adding: ',S)
                  else:
                    curCost = curPlan.cost(estimated=True)
                    if curCost < self.getPlanCost(S):
                      optPlan[S] = curPlan
                      self.addPlanCost(S, curCost)
                      # print('Updating: ',S)
                    else:
                      # print('Not added.')
                      pass
    return optPlan[frozenset(relationIds)]

if __name__ == "__main__":

  print('BushyOptimizer __main__')
  import doctest
  doctest.testmod(verbose=True)
