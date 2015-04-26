import Database
db = Database.Database()
try:
   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
except ValueError:
   pass
### SELECT * FROM employee JOIN department ON id = eid
query4 = db.query().fromTable('employee').join( \
      db.query().fromTable('department'), \
      method='block-nested-loops', expr='id == eid').finalize()

db.optimizer.pickJoinOrder(query4)