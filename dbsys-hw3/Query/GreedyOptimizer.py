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


class GreedyOptimizer(Optimizer):
  
  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    # print('GreedyJoin')
    # print(plan.flatten())

    self.totalCombosTried    = 0
    self.totalPlansProcessed = 0

    # optPlan = dict()
    todo = set() # queue of (sets of relations) to be processed
    relationIds = set(plan.relations())

    # add all relations
    for r in relationIds:
      new_op = TableScan(r,self.db.relationSchema(r))
      new_op.prepare(self.db)
      todo.add( tuple([frozenset(r), new_op] )) 

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


    # print('JoinExps: ',joinsExps)
    # print('Relations: ', relationIds)
    # print('Todo: ',todo)

    n = len(relationIds)

    for i in range(n,1,-1):

      curBestPair = None
      curBestPlan = None
      curBestCost = float('inf')

      for possible_join_pair in k_subsets(todo,2):
        (left, right)  = possible_join_pair
        # right = list(possible_join_pair)[1]

        # print('left:  ', left)
        # print('right: ', right)
        S = frozenset(left[0].union(right[0]))
        # print('S:     ', S)
        for t in left[0]:
          
          self.totalCombosTried += 1

          je = joinsExps[frozenset({t})]
          if (je==None):
            # print('Invalid Join')
            continue
          else: 

            temp = frozenset({t}).union(right[0])
            # print('temp: ',temp)
            for join_expression in je:
              if not join_expression[0].issubset(temp):
                # print('Invalid Join')
                continue
      
              # print('valid join between: ', left, right, ' at ', t,' in ', temp)

              self.totalPlansProcessed += 1
              curPlan = Join(left[1],right[1], 
                    expr=join_expression[1].joinExpr,
                    method='block-nested-loops')
              curPlan.prepare(self.db)

              curCost = curPlan.cost(estimated=False)

              if curCost < curBestCost:
                curBestPair = frozenset(left[0].union(right[0]))
                curBestLeft = frozenset(left[0])
                curBestRight = frozenset(right[0])

                curBestPlan = curPlan
                curBestCost = curCost
                # print('curBestPlan: ',curBestPlan)
                # print('S:  ', S)


      if curBestPair != None:
        # print('curBestPlan: ',curBestPlan)
        # print('curBestPair: ',curBestPair)
        # print('curBestLeft:  ',curBestLeft)
        # print('curBestRight: ',curBestRight)
      
        todo = set( [(S, plan) for S, plan in todo if ( not S.issubset(curBestLeft) and not S.issubset(curBestRight))] )
        # print('\nintermediate: ',todo,'\n')
        todo.add( (curBestPair, curBestPlan) )

        # print('newTodo: ',todo)

    # print('Final: ',todo)
    # assert len(todo) == 1
    # print('Final: ',next(iter(todo)))
    return next(iter(todo))

if __name__ == "__main__":

  print('GreedyOptimizer __main__')
  import doctest
  doctest.testmod(verbose=True)
