from __future__ import print_function
import sqlparse
import csv
import sys
import re
import itertools
meta_table = {}
table_dict = {}
cond_join, index_lhs, index_rhs, star_query = False, -1, -1, False

def query_split(query):
	if query == "":
		sys.exit("No input query")
	parsed = sqlparse.parse(query)[0]
	select_pos = 0
	col_pos = 2
	from_pos = 4
	table_pos = 6
	cond_pos = 8
	if "distinct" in query.lower():
		col_pos, table_pos, cond_pos, from_pos = col_pos + 2, table_pos + 2, cond_pos + 2, from_pos + 2

	if ("select" != str(parsed.tokens[select_pos]).lower()) or ("from" != str(parsed.tokens[from_pos]).lower()):
		sys.exit("Wrong Query Format")
	if (';' != str(parsed.tokens[-1])[-1]):
		sys.exit("Missing ; at end of query")

	columns = str(parsed.tokens[col_pos])
	columns = columns.replace(" ","")
	columns = columns.split(',')

	tables = str(parsed.tokens[table_pos])
	tables = tables.replace(" ","")
	tables = tables.split(',')

	conditions = []
	if len(parsed.tokens) > cond_pos:
		conditions = str(parsed.tokens[cond_pos])
		conditions = conditions.split()[1:]
		conditions[-1] = conditions[-1][:-1]

	return columns, tables, conditions

def str2intf(num):
	try:
		return int(num)
	except:
		return float(num)

def join_tables(tables):
	table_cols = []
	table = []
	for idx, tablename in enumerate(tables):
		if tablename not in meta_table:
			sys.exit("Table " + tablename + " does not exist")
		table_cols = table_cols + meta_table[tablename]
		if table == []:
			table = table_dict[tablename]
		else:
			temp = table_dict[tablename]
			table = list(itertools.product(table, temp))
			temp = []
			for elem in table:
				elem = elem[0] + elem[1]
				temp.append(elem)
			table = temp
	return table_cols, table


def select_columns(columns, table_cols):
	global star_query
	selected_idx = []
	for col in columns:
		flag = 0
		if col == "*":
			star_query = True
			selected_idx = selected_idx + list(xrange(len(table_cols)))
			continue
		flag = 0
		for idx, col_name in enumerate(table_cols):
			if col == col_name or col == col_name.split('.')[-1]:
				selected_idx.append(idx)
				flag += 1
		if flag == 0:
			sys.exit("Column name " + col + " does not exist")
		if flag > 1:
			sys.exit("Column name " + col + " is ambiguous")
	return selected_idx

def print_aggregte(func, value, col_name):
	print(func + "(" + col_name + ")")
	print(value)

aggs = ["max", "min", "sum", "avg", "count"]

def aggregate_query(column_inp, table_cols, final_table):
	splitted = column_inp.split("(")
	function = splitted[0]
	curr_col = splitted[1][:-1]
	flag = 0
	final_table = zip(*final_table)
	for idx, col_name in enumerate(table_cols):
		if curr_col == col_name or curr_col == col_name.split('.')[-1]:
			flag = 1
			if function.lower() == "max":
				value = max(final_table[idx])
			elif function.lower() == "sum":
				value = sum(final_table[idx])
			elif function.lower() == "min":
				value = min(final_table[idx])
			elif function.lower() == "avg" or function.lower() == "average":
				value = sum(final_table[idx], 0.0) / len(final_table[idx])
			elif function.lower() == "count":
				value = len(final_table[idx])
			else:
				sys.exit("Wrong function input")
			print_aggregte(function, value, col_name)
	if flag == 1:
		return True
	else:
		sys.exit("Some issue occured")

def print_final_table(header_list, rows_list):
	global star_query, cond_join, index_lhs, index_rhs
	del_idx = -1
	if star_query and cond_join:
		if index_lhs <= index_rhs:
			del_idx = index_rhs
		else:
			del_idx = index_lhs
		del header_list[del_idx]
	print(','.join(header_list))
	for row in rows_list:
		if star_query and cond_join:
			row = list(row)
			del row[del_idx]
		for idx, elem in enumerate(row):
			if idx!=len(row)-1:
				print(str(elem) + "," , end='')
			else:
				print(elem)

def get_selected_cols(selected_cols, table_cols, final_table):
	selected_col_list = []
	selected_table = []
	for idx in selected_cols:
		selected_col_list.append(table_cols[idx])
		if final_table != []:
			selected_table.append(final_table[idx])
	selected_table = zip(*selected_table)
	return selected_col_list, selected_table

def distinct_query(selected_table):
	ordered_list = list()
	map(lambda x: not x in ordered_list and ordered_list.append(x), selected_table)
	return ordered_list

conditions = [">=", "<=", ">", "<", "="]

def split_condition(condition):
	sign = ''
	if ">=" in condition:
		condition = condition.split(">=")
		sign = ">="
	elif "<=" in condition:
		condition = condition.split("<=")
		sign = "<="
	elif ">" in condition:
		condition = condition.split(">")
		sign = ">"
	elif "<" in condition:
		condition = condition.split("<")
		sign = "<"
	elif "=" in condition:
		condition = condition.split("=")
		sign = "="

	lhs = condition[0]
	rhs = condition[1]

	return lhs, sign, rhs

def get_type(val):
	try:
		return str2intf(val)
	except:
		return "col"

def check_cond(l, sign, r):
	if sign==">=":
		return (l>=r)
	elif sign=="<=":
		return (l<=r)
	elif sign==">":
		return (l>r)
	elif sign=="<":
		return (l<r)
	elif sign == "=":
		return (l==r)

def conditional_join(condition, table_cols, final_table):
	global cond_join, index_lhs, index_rhs
	lhs_c, sign, rhs_c = condition
	cnt_lhs, cnt_rhs = 0, 0
	for idx, col in enumerate(table_cols):
		if lhs_c == col or lhs_c == col.split('.')[-1]:
			index_lhs = idx
			cnt_lhs+=1
		if rhs_c == col or rhs_c == col.split('.')[-1]:
			index_rhs = idx
			cnt_rhs+=1

	if index_lhs == (-1) or index_rhs == (-1):
		sys.exit("Wrong condition variables")
	if (cnt_lhs > 1) or (cnt_rhs > 1):
		sys.exit("Ambiguous condition variables")
	temp = []

	if sign == "=":
		cond_join = True
	for row in final_table:
		if check_cond(row[index_lhs], sign, row[index_rhs]):
			temp.append(row)
	return temp, table_cols

def switch_sign(sign):
	if sign==">=":
		return "<="
	elif sign=="<=":
		return ">="
	elif sign==">":
		return "<"
	elif sign=="<":
		return ">"
	elif sign == "=":
		return "="

def conditional_query(table_cols, final_table, cond1, cond2, add_type = None):
	if add_type == None:
		cond2 = cond1
		add_type = "and"
	flag1, flag2 = False, False
	for cond in conditions:
		if cond in cond1:
			flag1 = True
		if cond in cond2:
			flag2 = True
	if (flag1 == False) or (flag2 == False):
		sys.exit("Wrong condition format")
	lhs_c1, sign_c1, rhs_c1 = split_condition(cond1)
	lhs_c2, sign_c2, rhs_c2 = split_condition(cond2)
	type_lhsc1, type_rhsc1, type_lhsc2, type_rhsc2 = get_type(lhs_c1), get_type(rhs_c1), get_type(lhs_c2), get_type(rhs_c2)
	if type_lhsc1 == type_rhsc1:
		if type_lhsc1 != "col":
			sys.exit("Condition format not supported")
		else:
			temp, table_cols = conditional_join((lhs_c1, sign_c1, rhs_c1), table_cols, final_table)
			return temp, table_cols
	if type_lhsc2 == type_rhsc2:
		if type_lhsc2 != "col":
			sys.exit("Condition format not supported")
	
	if type_lhsc1!="col":
		temp = lhs_c1
		lhs_c1 = rhs_c1
		rhs_c1 = temp
		sign_c1 = switch_sign(sign_c1)

	if type_lhsc2!="col":
		temp = lhs_c2
		lhs_c2 = rhs_c2
		rhs_c2 = temp
		sign_c2 = switch_sign(sign_c2)
	rhs_c1 = str2intf(rhs_c1)
	rhs_c2 = str2intf(rhs_c2)

	index_c1, index_c2, cnt_c1, cnt_c2 = (-1), -1, 0, 0
	for idx, col in enumerate(table_cols):
		if lhs_c1 == col or lhs_c1 == col.split('.')[-1]:
			index_c1 = idx
			cnt_c1+=1
		if lhs_c2 == col or lhs_c2 == col.split('.')[-1]:
			index_c2 = idx
			cnt_c2+=1

	if index_c1 == (-1) or index_c2 == (-1):
		sys.exit("Wrong condition variables")
	if(cnt_c1 > 1 or cnt_c2 > 1):
		sys.exit("Ambiguous condition variables")
	temp = []
	for row in final_table:
		if add_type.lower() == "and":
			if check_cond(row[index_c1], sign_c1, rhs_c1) and check_cond(row[index_c2], sign_c2, rhs_c2):
				temp.append(row)
		elif add_type.lower() == "or":
			if check_cond(row[index_c1], sign_c1, rhs_c1) or check_cond(row[index_c2], sign_c2, rhs_c2):
				temp.append(row)			

	return temp, table_cols

def parse_query(query):
	columns, tables, conditions = query_split(query)
	if conditions != []:
		if "where" not in query.lower():
			sys.exit("Wrong Query Format")
	if len(columns) == 1 and "(" in columns[0]:
		table_cols, final_table = join_tables(tables)
		for agg in aggs:
			if agg in columns[0]:
				out = aggregate_query(columns[0], table_cols, final_table)
				if out == 1:
					return True
	table_cols, final_table = join_tables(tables)
	flag = 0
	add_type = None
	if conditions != []:
		cond1, cond2 = '', ''
		for elem in conditions:
			if elem.lower() == "and" or elem.lower() == "or":
				add_type = elem
				flag=1
				continue
			if flag == 0:
				cond1 = cond1 + elem
			else:
				cond2 = cond2 + elem
		final_table, table_cols = conditional_query(table_cols, final_table, cond1, cond2, add_type)
	final_table = zip(*final_table)
	selected_cols = select_columns(columns, table_cols)
	selected_col_list, selected_table = get_selected_cols(selected_cols, table_cols, final_table)
	if "distinct" in query.lower().split():
		selected_table = distinct_query(selected_table)
	print_final_table(selected_col_list, selected_table)


def read_meta_file():
	tablename = ''
	begin_check = False
	with open('files/metadata.txt') as f:
		for line in f:
			line = line.strip()
			if begin_check:
				begin_check = False
				meta_table[line] = []
				tablename = line
			elif "<begin_table>" == line:
				begin_check = True
			elif "<end_table>" == line:
				pass
			else:
				meta_table[tablename].append(tablename + '.' + line)

def create_table(tablename):
	try:
		with open("files/" + tablename + ".csv") as f:
			csvData = csv.reader(f)
			rows = []
			for line in csvData:
				results = list(map(str2intf, line))
				rows.append(results)
			if len(rows[0]) != len(meta_table[tablename]):
				sys.exit("Something wrong with meta_table")
			return rows
	except:
		sys.exit("Some error occured in reading file")

def main():
	if len(sys.argv) != 2:
		sys.exit("USAGE ::: python " + sys.argv[0] + " <sql_query>")
	query = sys.argv[1]
	read_meta_file()
	for tablename in meta_table:
		table_dict[tablename] = create_table(tablename)

	out = parse_query(query)

if __name__ == '__main__':
	main()