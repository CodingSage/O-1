package edu.buffalo.cse562.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
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

public class ExpressionEvaluator implements ExpressionVisitor {

	List<String> columnNames = new ArrayList<String>();

	private double dval;
	private long l;
	private String s;
	private Date date;
	private boolean res;

	public List<String> getResult() {
		return columnNames;
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg) {
		// TODO Auto-generated method stub
		dval = Double.MAX_VALUE;
		l = Long.MAX_VALUE;
		arg.getLeftExpression().accept(this);

		if (l != Long.MAX_VALUE) // am a double
		{
			long lval = l;
			arg.getRightExpression().accept(this);
			l = l + lval;
		}

		arg.getLeftExpression().accept(this);
		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg.getRightExpression().accept(this);
			dval = lval + dval;
		}
	}

	@Override
	public void visit(Division arg) {
		
		dval = Double.MAX_VALUE;
		l = Long.MAX_VALUE;
		arg.getLeftExpression().accept(this);

		if (l != Long.MAX_VALUE) // am a long
		{
			long lval = l;
			arg.getRightExpression().accept(this);
			l = l / lval;
		}

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg.getRightExpression().accept(this);
			dval = lval / dval;
		}

	}

	@Override
	public void visit(Multiplication arg) {
		
		dval = Double.MAX_VALUE;
		l = Long.MAX_VALUE;
		arg.getLeftExpression().accept(this);

		if (l != Long.MAX_VALUE) // am a double
		{
			long lval = l;
			arg.getRightExpression().accept(this);
			l = l * lval;
		}

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg.getRightExpression().accept(this);
			dval = lval * dval;
		}
	}

	@Override
	public void visit(Subtraction arg) {
		
		dval = Double.MAX_VALUE;
		l = Long.MAX_VALUE;
		arg.getLeftExpression().accept(this);

		if (l != Long.MAX_VALUE) // am a double
		{
			long lval = l;
			arg.getRightExpression().accept(this);
			l = l - lval;
		}

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg.getRightExpression().accept(this);
			dval = lval - dval;
		}

	}

	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(EqualsTo arg) {
		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg.getRightExpression().accept(this);
			res = (dval == lval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg.getRightExpression().accept(this);
			res = (lval.equals(s));
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg.getRightExpression().accept(this);
			res = (lval == l);
		}

		if (date != null) {
			Date ldate = date;
			arg.getRightExpression().accept(this);

			if (date.compareTo(ldate) == 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg0.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg0.getRightExpression().accept(this);
			res = (lval > dval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg0.getRightExpression().accept(this);
			res = (lval.compareTo(s) > 0);
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg0.getRightExpression().accept(this);
			res = (lval > l);
		}

		if (date != null) {
			Date ldate = date;
			arg0.getRightExpression().accept(this);

			if (ldate.compareTo(date) > 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub

		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg0.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg0.getRightExpression().accept(this);
			res = (lval >= dval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg0.getRightExpression().accept(this);
			res = (lval.compareTo(s) >= 0);
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg0.getRightExpression().accept(this);
			res = (lval >= l);
		}

		if (date != null) {
			Date ldate = date;
			arg0.getRightExpression().accept(this);

			if (ldate.compareTo(date) >= 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg0.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg0.getRightExpression().accept(this);
			res = (lval < dval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg0.getRightExpression().accept(this);
			res = (lval.compareTo(s) < 0);
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg0.getRightExpression().accept(this);
			res = (lval < l);
		}

		if (date != null) {
			Date ldate = date;
			arg0.getRightExpression().accept(this);

			if (ldate.compareTo(date) < 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub

		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg0.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg0.getRightExpression().accept(this);
			res = (lval <= dval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg0.getRightExpression().accept(this);
			res = (lval.compareTo(s) <= 0);
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg0.getRightExpression().accept(this);
			res = (lval <= l);
		}

		if (date != null) {
			Date ldate = date;
			arg0.getRightExpression().accept(this);

			if (ldate.compareTo(date) <= 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		dval = Double.MAX_VALUE;
		s = null;
		l = Long.MAX_VALUE;
		date = null;

		arg0.getLeftExpression().accept(this);

		if (dval != Double.MAX_VALUE) // am a double
		{
			double lval = dval;
			arg0.getRightExpression().accept(this);
			res = (lval != dval);
		}

		if (s != null) // am a string
		{
			String lval = s;
			arg0.getRightExpression().accept(this);
			res = (lval.compareTo(s) != 0);
		}

		if (l != Long.MAX_VALUE) // am a long
		{
			Long lval = l;
			arg0.getRightExpression().accept(this);
			res = (lval != l);
		}

		if (date != null) {
			Date ldate = date;
			arg0.getRightExpression().accept(this);

			if (ldate.compareTo(date) != 0)
				res = true;
			else
				res = false;
		}

	}

	@Override
	public void visit(Column col) {
		columnNames.add(col.getColumnName());
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

}
