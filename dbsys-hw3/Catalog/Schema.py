import json, re
from collections import namedtuple, OrderedDict
from struct import Struct

class Types:
  """
  Utility functions for database types.

  The 'types' dictionary defines a mapping from user-facing type
  primitives to their representation in the Python 'struct' module,
  and a boolean indicating whether the type requires a repeat count prefix.

  The list of supported types in the database is given by the keys
  of the 'types' dictionary.
  """
  types = {
      # name, pack_letter, needs_len, default_val, from_string
      'byte'    : ('B', False, 0, lambda x: x),
      'short'   : ('h', False, 0, lambda x: int(x)),
      'int'     : ('i', False, 0, lambda x: int(x)),
      'float'   : ('f', False, 0.0, lambda x: float(x)),
      'double'  : ('d', False, 0.0, lambda x: float(x)),
      'char'    : ('s', True, chr(0), lambda x: x),
      'text'    : ('s', True, chr(0), lambda x: x)
    }

  @classmethod
  def parseType(cls, typeDesc):
    typeMatcher = re.compile("(?P<typeStr>\w+)(\((?P<size>\d+)\))?(?P<rest>.*)")
    match = typeMatcher.match(typeDesc)
    if match:
      return match.groupdict()

  @classmethod
  def formatType(cls, typeDesc):
    """
    Converts a type description string into a C-struct format.

    >>> Types.formatType('int')
    'i'

    Character sequences require a fixed-length declaration.

    >>> Types.formatType('char(100)')
    '100s'

    Invalid type description examples.

    >>> Types.formatType('int(100)') == None
    True
    >>> Types.formatType('char') == None
    True
    >>> Types.formatType('char(100') == None
    True
    >>> Types.formatType('char100)') == None
    True
    >>> Types.formatType('char(100)asdsa') == None
    True
    """
    format = None
    matches = Types.parseType(typeDesc)
    if matches:
      typeStr = matches.get("typeStr", None)
      size    = matches.get("size", None)
      rest    = matches.get("rest", None)
      if not rest:
        (format, requiresSize, _, _) = Types.types.get(typeStr, (None, None, None, None))
        if requiresSize:
          format = size+format if size else None
        else:
          format = format if not size else None

    return format


  @classmethod
  def defaultValue(cls, typeDesc):
    """
    Returns a default value for the given type.

    >>> Types.defaultValue('int') == 0
    True
    >>> Types.defaultValue('int(100)') == None
    True
    >>> Types.defaultValue('float') == 0.0
    True
    >>> Types.defaultValue('double') == 0.0
    True
    >>> Types.defaultValue('char(100)') == (chr(0) * 0)
    True
    """
    default = None
    matches = Types.parseType(typeDesc)
    if matches:
      typeStr = matches.get("typeStr", None)
      size    = matches.get("size", None)
      rest    = matches.get("rest", None)
      if not rest:
        (_, requiresSize, val, _) = Types.types.get(typeStr, (None, None, None, None))
        if requiresSize:
          default = val * 0 if size else None
        else:
          default = val if not size else None

    return default


  @classmethod
  def formatValue(cls, value, typeDesc, forSerialization=True):
    """
    Performs any type conversion necessary to process the given
    value as the given type during serialization and deserialization.

    For now, this converts character sequences from Python strings
    into bytes for Python's struct module.
    """
    prefixes = ['char', 'text']
    if list(filter(typeDesc.startswith, prefixes)):
      if forSerialization:
        return value.encode() if isinstance(value, str) else value
      else:
        return (value.decode() if isinstance(value, bytes) else value).rstrip("\x00 \n")
    else:
      return value

  @classmethod
  def valueFromString(cls, string, typeDesc):
    """
    Convert a string to a value given its desired type
    """
    # parse the type string typeStr, size
    matches = Types.parseType(typeDesc)
    if matches:
      typeStr = matches.get("typeStr", None)
      size    = matches.get("size", None)
      rest    = matches.get("rest", None)
      if not rest and typeStr:
        (_, requiresSize, val, conv_lambda) = Types.types.get(typeStr, (None, None, None, None))
        if requiresSize:
          if size:
            rem_length = int(size) - len(string)
            string = string + val * rem_length
          else: None
        return conv_lambda(string)

class DBSchema:
  """
  A database schema class to represent the type of a relation.

  Schema definitions require a name, and a list of attribute-type pairs.

  This schema class maintains the above information, as well as Python
  'namedtuple' and 'struct' instances to provide an in-memory object and
  binary serialization/deserialization facilities.

  That is, a Python object corresponding to an instance of the schema can
  easily be created using our 'instantiate' method.

  >>> schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])

  >>> e1 = schema.instantiate(1, '1990-01-01', 100000)
  >>> e1
  employee(id=1, dob='1990-01-01', salary=100000)

  Also, we can serialize/deserialize the created instances with the 'pack'
  and 'unpack' methods.

  (Note the examples below escape the backslash character to ensure doctests
  run correctly. These escapes should be removed when copy-pasting into the Python REPL.)

  >>> schema.pack(e1)
  b'\\x01\\x00\\x00\\x001990-01-01\\x00\\x00\\xa0\\x86\\x01\\x00'
  >>> schema.unpack(b'\\x01\\x00\\x00\\x001990-01-01\\x00\\x00\\xa0\\x86\\x01\\x00')
  employee(id=1, dob='1990-01-01', salary=100000)

  >>> e2 = schema.unpack(schema.pack(e1))
  >>> e2 == e1
  True

  Finally, the schema description itself can be serialized with the packSchema/unpackSchema
  methods. One example use-case is in our self-describing storage files, where the files
  include the schema of their data records as part of the file header.
  >>> schemaDesc = schema.packSchema()
  >>> schema2 = DBSchema.unpackSchema(schemaDesc)
  >>> schema.name == schema2.name and schema.schema() == schema2.schema()
  True

  # Test default tuple generation
  >>> d = schema.default()
  >>> d.id == 0 and d.dob == (chr(0) * 0) and d.salary == 0
  True

  >>> projectedSchema = DBSchema('employeeId', [('id', 'int')])
  >>> schema.project(e1, projectedSchema)
  employeeId(id=1)

  >>> projectedSchema.unpack(schema.projectBinary(schema.pack(e1), projectedSchema))
  employeeId(id=1)

  >>> schema.match(DBSchema('employee2', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')]))
  True
  """

  def __init__(self, name, fieldsAndTypes):
    self.name = name
    if self.name and fieldsAndTypes:
      self.fields  = [x[0] for x in fieldsAndTypes]
      self.types   = [x[1] for x in fieldsAndTypes]
      self.clazz   = namedtuple(self.name, self.fields)
      self.binrepr = Struct(''.join([Types.formatType(x) for x in self.types]))
      self.size    = self.binrepr.size
    else:
      raise ValueError("Invalid attributes when constructing a schema")

  # Returns a human-readable representation of this schema.
  def toString(self):
    fields = map(lambda x: '(' + ','.join(x) + ')', zip(self.fields, self.types))
    return self.name + '[' + ','.join(fields) + ']'

  def valuesFromStrings(self, *args):
    #print(self.types)
    #print(args)
    s_t = list(zip(args[0], self.types))
    #print(s_t)
    ret = list(map(lambda x: Types.valueFromString(x[0], x[1]), s_t))
    #print(ret)
    return ret

  # Returns a new schema with renamed attributes.
  # The arguments are a new schema name and a renaming dictionary
  # from current name to new name, e.g. for a schema with fields['a', 'b']:
  # attrNameMap = {'a': 'a2', 'b': 'b2'}
  def rename(self, schemaName, attrNameMap):
    newFields = [attrNameMap[x] for x in self.fields]
    return DBSchema(schemaName, list(zip(newFields, self.types)))

  # Return a list of fields and types of the schema
  def schema(self):
    if self.fields and self.types:
      return list(zip(self.fields, self.types))

  # Return a namedtuple representing a default instance of the schema
  def default(self):
    if self.clazz:
      return self.clazz(*map(Types.defaultValue, self.types))

  # Return an instance of the schema
  def instantiate(self, *args):
    if self.clazz:
      return self.clazz(*args)

  # Returns whether this schema matches another based on fields and types alone.
  def match(self, other):
    return list(zip(self.fields, self.types)) \
              == list(zip(other.fields, other.types))

  # Project a tuple to the given schema
  def project(self, instance, schema):
    fields = []
    for f in schema.fields:
      if f in self.fields:
        fields.append(getattr(instance, f))
      else:
        raise ValueError("Invalid field in projection: "+f)
    return schema.instantiate(*fields)

  # Project a packed tuple to a binary representation of the given schema.
  # TODO: make this more efficient by direct field access and copying.
  def projectBinary(self, binaryInstance, schema):
    return schema.pack(self.project(self.unpack(binaryInstance), schema))

  # Return a binary representation of the instance
  def pack(self, instance):
    if self.binrepr:
      values = [Types.formatValue(instance[i], self.types[i])
                  for i in range(len(instance))]
      return self.binrepr.pack(*values)

  def unpack(self, buffer):
    if self.clazz and self.binrepr:
      values = [Types.formatValue(v, self.types[i], False)
                  for i, v in enumerate(self.binrepr.unpack(buffer))]
      return self.clazz._make(values)

  def packSchema(self):
    return json.dumps(self, cls=DBSchemaEncoder).encode()

  @classmethod
  def unpackSchema(cls, buffer):
    return json.loads(buffer.decode(), cls=DBSchemaDecoder)


class DBSchemaEncoder(json.JSONEncoder):
  """
  Custom JSON encoder for serializing DBSchema objects.

  >>> schema = DBSchema('employee', [('id', 'int'), ('salary', 'int')])
  >>> json.dumps(schema, cls=DBSchemaEncoder)
  '{"__pytype__": "DBSchema", "name": "employee", "schema": [["id", "int"], ["salary", "int"]]}'
  """
  def default(self, obj):
    if isinstance(obj, DBSchema):
      return OrderedDict([('__pytype__', 'DBSchema'), ('name', obj.name), ('schema', obj.schema())])
    else:
      return super().default(obj)


class DBSchemaDecoder(json.JSONDecoder):
  """
  Custom JSON decoder for deserializing DBSchema objects.

  >>> schema = DBSchema('employee', [('id', 'int'), ('salary', 'int')])

  # Test DBSchema dump/load
  >>> schema2 = json.loads(json.dumps(schema, cls=DBSchemaEncoder), cls=DBSchemaDecoder)
  >>> schema.name == schema2.name and schema.schema() == schema2.schema()
  True

  # Test dump/load for other Python types.
  >>> json.loads(json.dumps('foo'), cls=DBSchemaDecoder)
  'foo'

  >>> json.loads(json.dumps([('foo',1), ('bar',2)]), cls=DBSchemaDecoder)
  [['foo', 1], ['bar', 2]]
  """
  def __init__(self):
    json.JSONDecoder.__init__(self, object_hook=self.decodeDBSchema)

  def decodeDBSchema(self, objDict):
    if '__pytype__' in objDict and objDict['__pytype__'] == 'DBSchema':
      return DBSchema(objDict['name'], objDict['schema'])
    else:
      return objDict

if __name__ == "__main__":
    import doctest
    doctest.testmod()
