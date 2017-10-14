# -*- coding: utf-8 -*-
import csv
import sys
import re
import sqlparse
import os
from sys import exit

select_arg=[]
tables=[]
conditions=[]
and_flag=0
or_flag=0
join_flag=0
func_flag=0
distinct_flag=0


### main 

def main():
	query = (sys.argv[1])
	if 'exit' in query:
		return;
	elif 'quit' in query:
		return;
	elif len(query) == 0 :
		return
	else:
		process(query)


### checking table exists or not

def check_file(Table):
	if (os.path.isfile(Table)):
		return 0
	else :
		print Table," : Error table does't exist \r\n"
		exit()


### getting attributes of a table

def get_entities(table_name):

	fp1= open("metadata.txt", "r+")

	line=""
	for i in fp1:
		line=line+i;

	file=line.split('\n')

	attributes=[]

	index=file.index(table_name)+1

	while file[index]!='<end_table>':
		attributes.append(table_name+"."+file[index])
		index=index+1

	# print "entities :",attributes
	return attributes


def Check_table(s):
    try: 
        int(s)
        return False
    except ValueError:
        return True

### Attributes names combined with tables for comparision

def get_full_names(condition, Tables):

	print condition,Tables
	if '.' in condition[0]:
		head=[]
		for i in Tables :
			head.extend(get_entities(i))
		# print Tables
		# print head ,"83"
		if condition[0] in head :
			return condition
		else :
			print "Error : invalid condition",condition
			exit()

	flag=0
	if '.' not in condition[0]:
		for i in Tables:
			for j in get_entities(i):
				if i + '.' + condition[0] == j:
					condition[0] = i+'.'+condition[0]
					flag=1
					break
	rflag=0
	if(Check_table(condition[2])) :
		if '.' not in condition[2]:
			for i in Tables:
				for j in get_entities(i):
					if i + '.' + condition[2] == j:
						condition[2] = i+'.'+condition[2]
						rflag=1
						break
		else :
			if condition[2] in head :
				rflag = 1
		if rflag == 0 :
			print "Error : invalid condition",condition
			exit()
	if flag == 0 :
		print "Error : invalid condition",condition
		exit()
	return condition


def modify_select_arg(condition, Headers):

	for i in range(0,len(condition)) :
		if '.' in condition[i]:
			if condition[i].split('.')[0] not in tables :
				print "Invalid argument",condition[i].split('.')[0]
				return []

		else :
			for k in Headers:
					if k.split('.')[1] == condition[i]:
						condition[i] = k
						
	# print "modify : ",condition
	return condition


### Attributes names combined with tables

def get_list_of_contents(Tables, columns):

	distinct_tables=[]
	for i in Tables :
		if i not in distinct_tables :
			distinct_tables.append(i)

	headers = []

	for i in columns:
		if('.' in i):
			headers.append(i)

		elif('.' not in i):
			table_att = i.split('.')
			c_name = table_att[0]

			for i in distinct_tables:
				if i + '.' + c_name in get_entities(i):
					headers.append(i + '.' + c_name)

	# print "get_contents : ",headers
	return headers


### Merging two tables  intersection -> and operation , union -> or operation

def intersect(Res1,Res2) :
	count=0
	Result=[]
	for i in Res2 :
		if i in Res1 :
			Result.append(i)
			count+=1
	return Result

def union(Res1,Res2) :
	Res=set(tuple(x) for x in Res1)
	Result=Res1[:]
	for i in Res2 :
		if tuple(i) not in Res :
			Result.append(i)
	return Result	


### checking for errors in  select and from synatx

def error_inQuery(Query) :

	List = Query.split(' ')
	List =[ j for j in List if j != ""]
	# print List

	if List[0] != "select" and List[0] != "SELECT" :
		print "Error : No Select statement "
		return 1

	state=0
	proceed=0

	if len(List) < 4 :
		print "Error : Insufficient arguments " 
		print " Invalid Syntax "
		return 1
	if List[1] == ',' :
		print " Invalid Syntax "
		return 1

	for i in range(2,len(List)) :

		if state == 0 :
			if proceed == 0 :
				if List[i] == ',' :
					proceed = 1
				else :
					if List[i] == 'from' or  List[i] == 'FROM' :
						state = 1
						proceed = 0
					elif List[i-1] != 'distinct' and List[i-1] != 'DISTINCT':
						print " Invalid Syntax "
						return 1
					
			else :
				if List[i] == 'from' or  List[i] == 'FROM' :
					print " Invalid Syntax "
					return 1
				else :
					proceed = 0

		elif state == 1 :
			if proceed == 0 :
				if List[i] != ',' :
					if List[i] == 'where' or  List[i] == 'WHERE' :
						state = 2
						proceed = 0
					elif List[i-1] != 'from' and List[i-1] != 'FROM':
						print " Invalid Syntax "
						return 1
				else :
					proceed = 1
			else :
				if List[i] == 'where' or List[i] == 'WHERE' :
					print " Invalid Syntax "
					return 1
				else :
					proceed = 0
		else :
			return 0


### checking for errors in the query

def error_checking() :

	Entities=select_arg[:]
	Tables=tables[:]
	# print select_arg,Entities,conditions, " : within error"

	if len(Entities) == 0 or len(Tables) == 0 :
		print "Error : Insufficient arguments " 
		print " Invalid Syntax "
		return 1
	for i in Tables :
		if check_file(i+'.'+'csv') == 1 :
			return 1
	headers=[]
	for i in Tables :
		headers.extend(get_entities(i))

	if func_flag == 0 :
		for i in Entities :
			flag=0
			if '.' in i :
				if i not in headers :
					print "Error : "+i+" does't exist in the provided tables "
					return 1
				else :
					flag = 1
			else :
				for j in headers :
					if j.split('.')[1] == i :
						flag =1
			if flag == 0 and i != '*' :
				print "Error : "+i+" does't exist in the provided tables "
				return 1

	for i in Entities :
		flag = 0
		for j in headers :
			if i == j.split('.')[1] :
				if flag == 0 :
					flag =1
				else :
					print "Error : "+i+" Ambiguous Attribute"
					return 1
	if func_flag == 1 :
		if len(select_arg) > 2 :
			print "Error : Row Attributes Can't be used with Aggregate function"
			return 1  
		if '.' in Entities[1] :
			if Entities[1] not in headers :
				print "Error : "+i+" does't exist in the provided tables "
				return 1
		else :
			flag = 0
			for i in headers :
				if i.split('.')[1] == Entities[1] :
					flag = 1
					break
			if flag == 0 :
				print "Error : "+Entities[1]+" does't exist in the provided tables "
				return 1

def distinct_check(query) :

	if 'distinct' in query :
		query=query.replace("distinct","DISTINCT")

	if query.count("DISTINCT") > 1 :
		print "Error : Query can't be executed"
		return 1
	if 'DISTINCT' in query :
		if '(' in query and ')' in query :
			check=query[query.index('(')+1:query.index(')')]
			if ',' in check :
				print "Error : distinct can't be performed on combination of attributes"
				return 1

### query processing

def process(query):

	Query=query[:]
	Query = Query.replace(",", " , ")
	Query = Query.replace("(", " , ")
	Query = Query.replace(")", " ")
	Query = Query.replace(">=", " * ")
	Query = Query.replace("<=", " # ")
	Query = Query.replace("=", " = ")
	Query = Query.replace(">", " > ")
	Query = Query.replace("<", " < ")

	check = error_inQuery(Query)

	if check == 1:
		sys.exit("ERROR: Invalid Query " )

	check=distinct_check(query) 

	if check == 1:
		sys.exit("ERROR: Invalid Query " )

	parse(query)

	check = error_checking()
	if check == 1:
		sys.exit("ERROR: Invalid Arguments " )

	if len(conditions) == 0 and len(tables) == 1:
		execute_nowheresingletable()

	elif len(conditions) == 0 and len(tables) !=1:
		execute_nowhere_withtables()

	elif len(conditions) != 0 :
		exec_where()


### Selecting tuples based on the conditions specified

def select_tuples(Cartesian_product_list,condition,Headers) :

	left = condition[0]
	op = condition[1]
	right = condition[2]

	res=[]
	join=0

	index_of_lentity=Headers.index(left)

	if '.' in left and '.' in right :
		join=1
		index_of_rentity=Headers.index(right)

	if op == '=' :
		for i in Cartesian_product_list[0] :
			x = i.split(",")
			y = [ int(j) for j in x if j != "" ]
			if join == 1 :
				if y[index_of_lentity] == y[index_of_rentity] :
					res.append(y)

			else :
				if y[index_of_lentity] == int(right) :
					res.append(y)

	elif op == '>' :
		for i in Cartesian_product_list[0] :
			x = i.split(",")
			y = [ int(j) for j in x if j != "" ]
			if join == 1 :
				if y[index_of_lentity] > y[index_of_rentity] :
					res.append(y)
			else :
				if y[index_of_lentity] > int(right) :
					res.append(y)

	elif op == '<' :
		for i in Cartesian_product_list[0] :
			x = i.split(",")
			y = [ int(j) for j in x if j != "" ]
			if join == 1 :
				if y[index_of_lentity] < y[index_of_rentity] :
					res.append(y)
			else :
				if y[index_of_lentity] < int(right) :
					res.append(y)

	elif op == '*' :
		for i in Cartesian_product_list[0] :
			x = i.split(",")
			y = [ int(j) for j in x if j != "" ]
			if join == 1 :
				if y[index_of_lentity] >= y[index_of_rentity] :
					res.append(y)
			else :
				if y[index_of_lentity] >= int(right) :
					res.append(y)

	elif op == '#' :
		for i in Cartesian_product_list[0] :
			x = i.split(",")
			y = [ int(j) for j in x if j != "" ]
			if join == 1 :
				if y[index_of_lentity] <= y[index_of_rentity] :
					res.append(y)
			else :
				if y[index_of_lentity] <= int(right) :
					res.append(y)

	else :
		print "Invalid operation"
		exit()
	temp=[]
	temp.append(Headers[:])
	res=temp+res
	if join == 1 and op == '=':
		for i in res :
			del i[index_of_rentity]
		modify_select_arg(select_arg,res[0])

	# print "select tuples : ",res,select_arg
	return res


### SQL query with where clause 

def exec_where() :

	headers=[]
	for i in tables :
		entities=get_entities(i)
		for j in entities:
			headers.append(j)

	tables_contents = cartesian_product(tables)

	while len(tables_contents) > 1:
		a = tables_contents.pop()
		b = tables_contents.pop()
		c = projection(a, b)

		tables_contents.append(c)

	condition_left=[]
	condition_right=[]

	result=[]

	if and_flag == 1 or or_flag == 1 :

		if and_flag == 1:
			condition_left=conditions[:conditions.index("AND")]
			condition_right=conditions[conditions.index("AND")+1:]
		else :
			condition_left=conditions[:conditions.index("OR")]
			condition_right=conditions[conditions.index("OR")+1:]

		condition_left=get_full_names(condition_left,tables)
		condition_right=get_full_names(condition_right,tables)

		flag=0
		for i in condition_left :
			if '.' in i and i not in headers :
				flag=1
				break
		for i in condition_right :
			if '.' in i and i not in headers :
				flag=1
				break
		# print flag
		if flag == 0 :

			if '.' in condition_left[0] and '.' in condition_left[2] and condition_left[1] == '=':
				res2=select_tuples(tables_contents,condition_right,headers)
				res1=select_tuples(tables_contents,condition_left,headers)
				head=res2[0]
				index=head.index(condition_left[2])
				for i in res2 :
					del i[index]

			elif '.' in condition_right[0] and '.' in condition_right[2] and condition_right[1] == '=' :
				res1=select_tuples(tables_contents,condition_left,headers)	
				res2=select_tuples(tables_contents,condition_right,headers)
				head=res1[0]
				index=head.index(condition_right[2])
				for i in res1 :
					del i[index]

			else :
				res1=select_tuples(tables_contents,condition_left,headers)	
				res2=select_tuples(tables_contents,condition_right,headers)

			if and_flag == 1 :
				res=intersect(res1,res2)
			else :
				res=union(res1,res2)
		else :
			print " Invalid Condition : Argument does't exist in the table"

	else :

		condition_left=conditions[:]
		# print condition_left
		condition_left=get_full_names(condition_left,tables)
		print condition_left


		for i in condition_left :
			if '.' in i and i not in headers :
				print " Invalid Condition : Argument does't exist in the table"
				exit() 
		res=select_tuples(tables_contents,condition_left,headers)


	if func_flag == 1 :
		
		entity_agg=[]
		entity_agg.append(select_arg[1])

		if '.' in entity_agg[0] :
			exec_agg(entity_agg,res)
		else :
			for i in headers :
				if entity_agg[0] == i.split('.')[1] :
					temp=[]
					temp.append(i)
					exec_agg(temp,res)
					break

	elif distinct_flag == 1 :
		modify_select_arg(select_arg,res[0])
		select_distinct(select_arg,res)

	else :
		if select_arg[0] == '*' :
			# print res,res1,res2
			entities=res[0][:]
		else :
			entities=get_list_of_contents(tables,select_arg)
		exec_selection(entities,res)	



### operations on multiple tables and no join condition

def execute_nowhere_withtables() :

	tables_contents = cartesian_product(tables)
	while len(tables_contents) > 1:
		a = tables_contents.pop()
		b = tables_contents.pop()
		c = projection(a, b)

		tables_contents.append(c)

	entities = []
	for i in tables:
		entities.extend(get_entities(i))

	cartesian_product_list=[]
	cartesian_product_list.append(entities)

	for i in tables_contents[0]:
		x = i.split(",")
		y = [ j for j in x if j != "" ]
		cartesian_product_list.append(y)

	if select_arg[0] == '*' :
		exec_selection(entities,cartesian_product_list)

	elif select_arg[0] != '*' :

		headers = []
		headers = get_list_of_contents(tables, select_arg)

		if func_flag == 1 :
			exec_agg(headers,cartesian_product_list)	
		elif distinct_flag == 1 :
			modify_select_arg(select_arg,cartesian_product_list[0])
			select_distinct(select_arg,cartesian_product_list)
		else :
			exec_selection(headers,cartesian_product_list)


### operations on single table

def execute_nowheresingletable():

	tables_contents = cartesian_product(tables)
	file_name = tables[0]
	entities=get_entities(file_name)

	cartesian_product_list=[]
	cartesian_product_list.append(entities)

	for i in tables_contents[0]:
		x = i.split(",")
		y = [ j for j in x if j != "" ]
		cartesian_product_list.append(y)

	if select_arg[0] == '*' :
		exec_selection(entities,cartesian_product_list)

	else :
		headers = []
		headers = get_list_of_contents(tables, select_arg)

		if func_flag == 1:
			exec_agg(headers,cartesian_product_list)	
		elif distinct_flag == 1 :
			modify_select_arg(select_arg,cartesian_product_list[0])
			select_distinct(select_arg,cartesian_product_list)
		else :
			exec_selection(headers,cartesian_product_list)


### SQL query select clause

def exec_selection(Entities,Table) :
	index_of_entity=[]
	for i in Entities :
		index_of_entity.append(int(Table[0].index(i)))
	# print index_of_entity

	for i in Table :
		k=[ j for j in i if j != "" ]
		z=[]
		for j in index_of_entity :
			z.append(k[j])
		print " ,  ".join(str(j) for j in z)


### operating with distinct function

def select_distinct(Arguments,Listoflist) :
	index_of_entity=[]
	for i in Arguments :
		index_of_entity.append(Listoflist[0].index(i))

	del Listoflist[0]
	List=[]
	
	for i in Listoflist :
		subList=[]
		for j in index_of_entity :
			subList.append(i[j])
		List.append(subList)

	res=[]
	for i in List :
		if i not in res :
			res.append(i)
	print " , ".join( j for j in Arguments )
	for i in res :
		print " , ".join( str(j) for j in i )


### agregate functions with multiple tables

def exec_agg(Headers,Cartesian_product_list) :

	index_of_entity =Cartesian_product_list[0].index(Headers[0])
	
	List=[]
	for i in range(1,len(Cartesian_product_list)) :
		List.append(int(Cartesian_product_list[i][index_of_entity]))

	if select_arg[0] == 'SUM' :
		print sum(List)

	elif select_arg[0] == 'MIN' :
		print min(List)

	elif select_arg[0] == 'MAX' :
		print max(List)

	elif select_arg[0] == 'COUNT' :
		print len(List)

	elif select_arg[0] == 'AVG' :
		print sum(List)/float(len(List))

	else :
		print "invalid aggregate function"
		exit()


### cartesian product of all tables

def cartesian_product(Tables):
	result = []
	for i in Tables:
		i = i + ".csv"

		subresult = []
		with open(i, "r+") as fp2:
			j = fp2.readlines()		
		for k in j:
			subresult.append(k.strip("\r\n"))
		result.append(subresult)
		fp2.close()
	return result


### Cartisian product of two tables

def projection(table_a, table_b):
	result = []
	for i in table_a:
		for j in table_b:
			t = str(j) + ',' + str(i)
			result.append(t)
	return result


### Parsing the query 

def parse(query):
	try:
		# print "within parse"
		global select_arg
		global tables
		global conditions
		global and_flag
		global or_flag
		global join_flag
		global func_flag
		global distinct_flag

		query = query.replace(",", " ")
		query = query.replace("(", " ")
		query = query.replace(")", " ")
		query = query.replace(">=", " * ")
		query = query.replace("<=", " # ")
		query = query.replace("=", " = ")
		query = query.replace(">", " > ")
		query = query.replace("<", " < ")

		### checking for aggeregate functions
		func=['sum','min','max','MAX','MIN','SUM','avg','AVG','count','COUNT','DISTINCT','distinct']
		for i in func :
			if i in query :
				func_flag = 1

		List=[]
		List=query.split()

		key=['select','from','where','SELECT','FROM','WHERE','and','or','AND','OR']

		for i in List:
			if i in key or i in func :
				List[List.index(i)] = i.upper()

		select_arg=List[List.index("SELECT")+1:List.index("FROM")]

		if 'DISTINCT' in select_arg :
			del select_arg[select_arg.index("DISTINCT")]
			distinct_flag=1
			func_flag=0

		if "WHERE" in List :
			tables=List[List.index("FROM")+1:List.index("WHERE")]
			conditions=List[List.index("WHERE")+1:]
		else :
			tables=List[List.index("FROM")+1:]

		if "AND" in conditions : 
			and_flag=1
		if "OR" in conditions : 
			or_flag=1

	except:
		pass


if __name__ == "__main__" :
	main()
#print select_arg,tables,conditions,and_flag,or_flag,join_flag,func_flag,distinct_flag
