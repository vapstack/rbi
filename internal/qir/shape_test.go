package qir

import "testing"

func TestShapeWithoutOrderClearsInheritedOrder(t *testing.T) {
	shape := Shape{
		Expr:     Expr{Op: OpEQ, FieldOrdinal: 3, Value: "active"},
		Order:    Order{FieldOrdinal: 5, Kind: OrderKindBasic, Desc: true},
		HasOrder: true,
		Offset:   7,
		Limit:    11,
	}

	subQ := shape.WithExpr(Expr{Op: OpEQ, FieldOrdinal: 2, Value: "NL"}).WithWindow(0, 4).WithoutOrder()

	if subQ.HasOrder {
		t.Fatalf("expected WithoutOrder to clear HasOrder")
	}
	if subQ.Order != (Order{}) {
		t.Fatalf("expected WithoutOrder to zero order, got %+v", subQ.Order)
	}
	if subQ.Offset != 0 || subQ.Limit != 4 {
		t.Fatalf("expected WithoutOrder to preserve window override, got offset=%d limit=%d", subQ.Offset, subQ.Limit)
	}
	if subQ.Expr.FieldOrdinal != 2 || subQ.Expr.Value != "NL" {
		t.Fatalf("expected WithoutOrder to preserve expr override, got %+v", subQ.Expr)
	}
}
