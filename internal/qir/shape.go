package qir

// Shape is a stack-level execution shape over one prepared query.
// It may override root expr and window, but it never owns dynamic memory.
type Shape struct {
	Expr     Expr
	Order    Order
	HasOrder bool
	Offset   uint64
	Limit    uint64
}

func NewShape(query *Query) Shape {
	if query == nil {
		return Shape{}
	}
	shape := Shape{
		Expr:   query.Expr,
		Offset: query.Offset,
		Limit:  query.Limit,
	}
	if len(query.Order) == 1 {
		shape.Order = query.Order[0]
		shape.HasOrder = true
	}
	return shape
}

func (shape Shape) WithExpr(expr Expr) Shape {
	shape.Expr = expr
	return shape
}

func (shape Shape) WithWindow(offset, limit uint64) Shape {
	shape.Offset = offset
	shape.Limit = limit
	return shape
}

func (shape Shape) WithoutOrder() Shape {
	shape.Order = Order{}
	shape.HasOrder = false
	return shape
}
