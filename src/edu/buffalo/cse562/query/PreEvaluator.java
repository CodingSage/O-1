package edu.buffalo.cse562.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.model.Table;

public class PreEvaluator implements ExpressionVisitor {

	List<Table> tables;
	List<String> columnNames = new ArrayList<String>();
	private Table operand;
	private Evaluator eval;
	private double dval;
	private int val;
	private long l;
	private String s;
	private Date date;
	private boolean res, sum, avg, cnt;
	private boolean hasGroupBy;

	public PreEvaluator() {
	}

	public PreEvaluator(List<Table> tables) {
		this.tables = tables;
		List<String> names = new ArrayList<String>();
		for (Table table : tables)
			names.add(table.getName());
		eval = new Evaluator(operand);//, names);
		sum = false;
		avg = false;
		cnt = false;
		hasGroupBy = true;
	}

	public List<Table> getResult() {
		return tables;
	}

	@Override
	public void visit(NullValue arg0) {

	}

	@Override
	public void visit(Function arg0) {
	}

	@Override
	public void visit(InverseExpression arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg) {
		dval = arg.getValue();
	}

	@Override
	public void visit(LongValue arg) {
		l = arg.getValue();
	}

	@Override
	public void visit(DateValue arg) {
		date = arg.getValue();
	}

	@Override
	public void visit(TimeValue arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(TimestampValue arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(Parenthesis arg0) {
		Expression exp = arg0.getExpression();
		exp.accept(this);
	}

	@Override
	public void visit(StringValue arg) {
		// TODO Auto-generated method stub
		s = new String(arg.getValue());
	}

	public void addColumns(Expression a, Expression b) {

	}

	@Override
	public void visit(Addition arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(Division arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(Multiplication arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(Subtraction arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(AndExpression arg0) {
		try {
			if (arg0.getLeftExpression() instanceof Column)
				expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
			else if (arg0.getRightExpression() instanceof Column)
				expressionEvaluate(arg0, (Column) arg0.getRightExpression());
			else {
				arg0.getLeftExpression().accept(this);
				arg0.getRightExpression().accept(this);
			}
		} catch (Exception ex) {
			try {
				throw new Exception(arg0.toString() + "||" + ex.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void visit(OrExpression arg0) {
	}

	@Override
	public void visit(Between arg0) {
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
	}

	@Override
	public void visit(EqualsTo arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		else if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
		else {
			arg0.getLeftExpression().accept(this);
			arg0.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(InExpression arg0) {
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
	}

	@Override
	public void visit(IsNullExpression arg0) {
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
	}

	@Override
	public void visit(LikeExpression arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(MinorThan arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(Column col) {
		operand = operand.getColumn(col.getWholeColumnName());
		columnNames.add(col.getWholeColumnName());
	}

	@Override
	public void visit(SubSelect arg0) {

	}

	@Override
	public void visit(CaseExpression arg0) {

	}

	@Override
	public void visit(WhenClause arg0) {

	}

	@Override
	public void visit(ExistsExpression arg0) {
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		arg0.getSubSelect().accept(this);
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		arg0.getSubSelect().accept(this);
	}

	@Override
	public void visit(Concat arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(Matches arg0) {
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column)
			return;
		if (arg0.getLeftExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getLeftExpression());
		if (arg0.getRightExpression() instanceof Column)
			expressionEvaluate(arg0, (Column) arg0.getRightExpression());
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// expressionEvaluate(arg0);
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// expressionEvaluate(arg0);
	}

	private void expressionEvaluate(Expression arg, Column value) {
		String tName = value.getTable().getName();
		if(tName == null && tables.size() == 1)
			tName = tables.get(0).getName();
		for (Table t : tables)
			if (t.getName().equals(tName.toLowerCase())) {
				eval.setOperand(t);
				break;
			}
		if (eval.getOperand() == null)
			return;
		eval.reset();
		while (eval.hasNext()) {
			try {
				eval.next();
				LeafValue val = eval.eval(arg);
				if (!((BooleanValue) val).getValue())
					eval.remove();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public double getDval() {
		return dval;
	}

	public void setDval(double dval) {
		this.dval = dval;
	}

	public long getL() {
		return l;
	}

	public void setL(long l) {
		this.l = l;
	}

	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public boolean isRes() {
		return res;
	}

	public void setRes(boolean res) {
		this.res = res;
	}

	public boolean issum() {
		return sum;
	}

	public boolean isavg() {
		return avg;
	}

	public boolean iscnt() {
		return cnt;
	}

	public Table getOperand() {
		return operand;
	}

	public void setOperand(Table operand) {
		this.operand = operand;
	}
}
