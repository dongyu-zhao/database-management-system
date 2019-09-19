## Database Management System

This is a database management system written in C with fast speed.

Table should be saved in a .csv file, the file name is the table's name in the database. Each line in the file represents an record with n fields of the data in this table.

This database management system supports multiple tables and select (including multiple cross-table select, and to show results more easily, return the sum of all the data in required fields) queries.  
e.g.  
SELECT SUM(A.c14), SUM(A.c16), SUM(A.c33)  
FROM A, C, I, K  
WHERE A.c2 = C.c0 AND A.c8 = I.c0 AND A.c10 = K.c0
AND A.c24 < -7307;  

#### Highlight
* Use only 1 G memory but have the ability to handle database larger than the memory. (Handle out of memory problem)
* Use the design of column-oriented implementation and transfer original .csv format into binary for less space and high read speed.
* Implement both the nested_block and sort join algorithm for joining.
* Implement Selinger's cost-based query optimization as the optimizor of join order.
