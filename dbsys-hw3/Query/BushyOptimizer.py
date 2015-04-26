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
    # print(plan.flatten())
    print('Initial:\n',plan.explain())

    self.totalCombosTried    = 0
    self.totalPlansProcessed = 0

    optPlan = dict()   # (set of relations) -> best plan to join these relations
    relationIds = set(plan.relations())

    # add all relations
    for r in relationIds:
      r_set = frozenset({r})
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

    n = len(relationIds)
    for i in range(2,n+1):
      # for each subset of size i
      for S in [frozenset(S) for S in k_subsets(relationIds,i)]:
        print('S = ',S)
        # for each subset of S
        for j in range(1,int(i/2)+1):
          for O in [frozenset(O) for O in k_subsets(S,j)]:

            right = frozenset(S-O)
            print('O = ',O)
            print('combining',O,right)

            # if there is a relation joining something in Left to something in Right
            for t in O:

              self.totalCombosTried += 1
              je = joinsExps[frozenset({t})]
              if (je==None) or not (je[0].issubset(S)):
                print('Invalid Join')
                continue
              
              self.totalPlansProcessed += 1
              curPlan = Join(optPlan[O],optPlan[right], 
                    expr=je[1].joinExpr,
                    method='block-nested-loops')

              if S not in optPlan:
                optPlan[S] = curPlan
                self.addPlanCost(curPlan, curPlan.cost(estimated=False))
              else:
                curCost = curPlan.cost(estimated=False)
                if curCost < self.getPlanCost(optPlan[S]):
                  optPlan[S] = curPlan
                  self.addPlanCost(curPlan, curCost)

if __name__ == "__main__":

  print('BushyOptimizer __main__')
  import doctest
  doctest.testmod(verbose=True)
