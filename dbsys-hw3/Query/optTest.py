import Database
from Query.Optimizer       import Optimizer
from Query.BushyOptimizer  import BushyOptimizer
from Query.GreedyOptimizer import GreedyOptimizer
import timeit

from Query.Plan import Plan

db = Database.Database()
try:
   # db.createRelation('department', [('did', 'int'), ('eid', 'int')])
   # db.createRelation('employee',   [('id',  'int'), ('age', 'int')])
   db.createRelation('A', [('a1', 'int'), ('a2', 'int'), ('a3', 'int')])
   db.createRelation('B', [('b1', 'int'), ('b2', 'int'), ('b3', 'int')])
   db.createRelation('C', [('c1', 'int'), ('c2', 'int'), ('c3', 'int')])
   db.createRelation('D', [('d1', 'int'), ('d2', 'int'), ('d3', 'int')])
   db.createRelation('E', [('e1', 'int'), ('e2', 'int'), ('e3', 'int')])
   db.createRelation('F', [('f1', 'int'), ('f2', 'int'), ('f3', 'int')])
   db.createRelation('G', [('g1', 'int'), ('g2', 'int'), ('g3', 'int')])
   db.createRelation('H', [('h1', 'int'), ('h2', 'int'), ('h3', 'int')])
   db.createRelation('I', [('i1', 'int'), ('i2', 'int'), ('i3', 'int')])
   db.createRelation('J', [('j1', 'int'), ('j2', 'int'), ('j3', 'int')])
   db.createRelation('K', [('k1', 'int'), ('k2', 'int'), ('k3', 'int')])
   db.createRelation('L', [('l1', 'int'), ('l2', 'int'), ('l3', 'int')])

except ValueError as e:
    # print(e)
    pass
### SelecT * fROM employee jOiN department ON id = eid
query4 = db.query().fromTable('employee').join( \
      db.query().fromTable('department'), \
      method='block-nested-loops', expr='id == eid').finalize()

# db.optimizer.pickjoinOrder(query4)


query2= db.query().fromTable('A').join( \
          db.query().fromTable('B'), \
          method='block-nested-loops', expr='a1 == b1')

query4= query2.join(\
              db.query().fromTable('C').join( \
              db.query().fromTable('D'), \
              method='block-nested-loops', expr='c1 == d1'),
          method='block-nested-loops', expr='b1 == c1')

query6= query4.join(\
              db.query().fromTable('E').join( \
              db.query().fromTable('F'), \
              method='block-nested-loops', expr='e1 == f1'),
          method='block-nested-loops', expr='d1 == e1')

query8= query6.join(\
              db.query().fromTable('G').join( \
              db.query().fromTable('H'), \
              method='block-nested-loops', expr='g1 == h1'),
          method='block-nested-loops', expr='f1 == g1')

query10= query8.join(\
              db.query().fromTable('I').join( \
              db.query().fromTable('J'), \
              method='block-nested-loops', expr='i1 == j1'),
          method='block-nested-loops', expr='h1 == i1')

query12= query10.join(\
              db.query().fromTable('K').join( \
              db.query().fromTable('L'), \
              method='block-nested-loops', expr='k1 == l1'),
          method='block-nested-loops', expr='j1 == k1')

q2  = query2.finalize()
q4  = query4.finalize()
q6  = query6.finalize()
q8  = query8.finalize()
q10 = query10.finalize()
q12 = query12.finalize()


test_queries = (q2, q4)#, q6, q8, q10, q12)

print('test_queries:\n',test_queries)

print('\n\n==== LeftDeepOptimizer ====')
db.setQueryOptimizer(optimizer='LeftDeepOptimizer')
for (n,q) in enumerate(test_queries):
    print('Initial Query:\n',q.explain())

    # p = Plan(root=db.optimizer.pickJoinOrder(q))
    # print(p.explain())
    t = timeit.Timer('db.optimizer.pickJoinOrder(q)',"from __main__ import db,q",).timeit(1)
    stats = db.optimizer.getLatestOptimizationStats()
    print('t:', t, '\t c:',stats[0], '\t p:',stats[1] )


print('\n\n==== BushyOptimizer ====')
db.setQueryOptimizer(optimizer='BushyOptimizer')
for (n,q) in enumerate(test_queries):
    print('Initial Query:\n',q.explain())

    # p = Plan(root=db.optimizer.pickJoinOrder(q))
    # print(p.explain())
    t = timeit.Timer('db.optimizer.pickJoinOrder(q)',"from __main__ import db,q",).timeit(1)
    stats = db.optimizer.getLatestOptimizationStats()
    print('t:', t, '\t c:',stats[0], '\t p:',stats[1] )



print('\n\n==== GreedyOptimizer ====')
db.setQueryOptimizer(optimizer='GreedyOptimizer')
for (n,q) in enumerate(test_queries):
    print('Initial Query:\n',q.explain())

    # p = Plan(root=db.optimizer.pickJoinOrder(q))
    # print(p.explain())
    t = timeit.Timer('db.optimizer.pickJoinOrder(q)',"from __main__ import db,q",).timeit(1)
    stats = db.optimizer.getLatestOptimizationStats()
    print('t:', t, '\t c:',stats[0], '\t p:',stats[1] )    

# db.setQueryOptimizer(Optimizer(db))
# db.optimizer.pickjoinOrder(query4)

# db.setQueryOptimizer(bushyOptimizer(db))

# db.setQueryOptimizer(greedyOptimizer(db))







