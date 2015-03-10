from Storage.BufferPool import BufferPool
from Catalog.Schema import DBSchema
from Storage.StorageEngine import StorageEngine

schemas = [
        DBSchema('part', 
           [('p_partkey', 'int'),
           ('p_name',    'char(55)'),
           ('p_mfgr',    'char(25)'),
           ('p_brand',   'char(10)'),
           ('p_type',    'char(25)'),
           ('p_size',    'int'),
           ('p_container', 'char(10)'),
           ('p_retailprice', 'float'),
           ('p_comment', 'char(23)')])
        ,
        DBSchema('supplier',
            [('s_suppkey', 'int'),
            ('s_name', 'char(25)'),
            ('s_address', 'char(40)'),
            ('s_nationkey', 'int'),
            ('s_phone', 'char(15)'),
            ('s_acctbal', 'float'),
            ('s_comment', 'char(101)')])
        ,
        DBSchema('partsupp',
            [('ps_partkey', 'int'),
            ('ps_suppkey', 'int'),
            ('ps_availqty', 'int'),
            ('ps_supplycost', 'float'),
            ('ps_comment', 'char(199)')])
        ,
        DBSchema('customer',
            [('c_custkey', 'int'),
            ('c_name', 'char(25)'),
            ('c_nationkey', 'int'),
            ('c_phone', 'char(15)'),
            ('c_acctbal', 'float'),
            ('c_mktsegment', 'char(10)'),
            ('c_comment', 'char(117)')])
        ,
        DBSchema('orders',
            [('o_orderkey', 'int'),
            ('o_custkey', 'int'),
            ('o_orderstatus', 'char(1)'),
            ('o_totalprice', 'float'),
            ('o_orderdate', 'char(10)'), # really date
            ('o_orderpriority', 'char(15)'),
            ('o_clerk', 'char(15)'),
            ('o_shippriority', 'int'),
            ('o_comment', 'char(79)')])
        ,
        DBSchema('lineitem',
            [('l_orderkey', 'int'),
            ('l_partkey', 'int'),
            ('l_suppkey', 'int'),
            ('l_linenumber', 'int'),
            ('l_quantity', 'float'),
            ('l_extendedprice', 'float'),
            ('l_discount', 'float'),
            ('l_tax', 'float'),
            ('l_returnflag', 'char(1)'),
            ('l_linestatus', 'char(1)'),
            ('l_shipdate', 'char(10)'), # date
            ('l_commitdate', 'char(10)'), # date
            ('l_receiptdate', 'char(10)'), # date
            ('l_shipinstruct', 'char(25)'),
            ('l_shipmode', 'char(10)'),
            ('l_comment', 'char(44)')])
        ,
        DBSchema('nation',
            [('n_nationkey', 'int'),
            ('n_name', 'char(25)'),
            ('n_regionkey', 'int'),
            ('n_comment', 'char(152)')])
        ,
        DBSchema('region',
            [('r_regionkey', 'int'),
            ('r_name', 'char(25)'),
            ('r_comment', 'char(152)')])
        ]

storage = StorageEngine()

for schema in schemas:
  #print(schema.name)
  storage.createRelation(schema.name, schema)
  f = open(schema.name + ".csv", "r")
  count = 1
  for line in f:
    line = line[:-1]
    print(count)
    words = line.split('|')
    #print(words)
    vs = schema.valuesFromStrings(words)
    #print(vs)
    s = schema.instantiate(*vs)
    #print(s)
    tup = schema.pack(s)
    storage.insertTuple(schema.name, tup)
    count += 1
  f.close()
