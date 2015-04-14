import ast
import io
import Utils.unparse as unparse

# Extract information from an eval'able expression
class ExpressionInfo(ast.NodeVisitor):
  def __init__(self, expr):
    self.expr = expr
    self.names = []
    self.components = []
    self.onlyNames = True
    tree = ast.parse(self.expr)
    self.visit(tree)

  def visit_Expr(self, node):
    if not isinstance(node.value, ast.Name):
      self.onlyNames = False

    if isinstance(node.value, ast.BoolOp) and isinstance(node.value.op, ast.And):
      self.components = node.value.values

    ast.NodeVisitor.generic_visit(self, node)

  def visit_Name(self, node):
    self.names.append(node.id)
    ast.NodeVisitor.generic_visit(self, node)

  def getAttributes(self):
    return set(self.names)

  def decomposeCNF(self):
    result = []
    if self.components:
      for c in self.components:
        s = io.StringIO()
        unparse.Unparser(c,s)
        result.append(s.getvalue().strip())
    else:
      result = [self.expr]
    return result

  def isAttribute(self):
    return self.onlyNames
