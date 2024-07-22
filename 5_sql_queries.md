# 5 Interesting queries

## Query 1 - Selection - Find out highly paid instructors

### Instructor

``` SQL
select * from instructor where salary > 120000  limit 50;
```

### Output

| ID    | name     | dept_name | salary    |
|-|-|-|-|
| 19368 | Wieland  | Pol. Sci. | 124651.41 |
| 74420 | Voronina | Physics   | 121141.99 |
|........|........|........|........|


## Query 2 - Projection - Show only name and dept_name from Instructor and Student tables

### Instructor 
``` SQL
select name,dept_name from instructor limit 50;
```
### Output

| name              | dept_name   |
|-|-|
| Lembr             | Accounting  |
| Bawa              | Athletics   |
| Yazdi             | Athletics   |
| Wieland           | Pol. Sci.   |
| DAgostino         | Psychology  |
|........|........|


### Student
``` SQL
select name,dept_name from student limit 50;
```

### Output

| name              | dept_name   |
|-|-|
| Manber     | Civil Eng.  |
| Zelty      | Mech. Eng.  |
| Duan       | Civil Eng.  |
| Colin      | Civil Eng.  |
| Mediratta  | Geology     |
|........|........|


## Query 3 - Union - Get Names and departments of all Students and Instructors

### Instructor Union Student
``` SQL
SELECT name, dept_name FROM instructor UNION SELECT name, dept_name FROM student LIMIT 50;
```

### Output

| name              | dept_name   |
|-|-|
| Lembr             | Accounting  |
| Bawa              | Athletics   |
| Yazdi             | Athletics   |
| Wieland           | Pol. Sci.   |
| DAgostino         | Psychology  |
|........|........|
| Manber            | Civil Eng.  |
| Zelty             | Mech. Eng.  |
| Duan              | Civil Eng.  |
| Colin             | Civil Eng.  |
| Mediratta         | Geology     |
|........|........|

# Query 4 - Diff - Get Only the Instructors who are not students

``` SQL
SELECT name, dept_name FROM instructor EXCEPT  SELECT name, dept_name FROM student LIMIT 50;
```

### Output

| name              | dept_name   |
|-|-|
| Lembr             | Accounting  |
| Bawa              | Athletics   |
| Yazdi             | Athletics   |
|........|........|
| Bertolino         | Mech. Eng.  |
| Dale              | Cybernetics |

### This has removed below intersecting  data.

| name              | dept_name   |
|-|-|
| Bondi    | Comp. Sci. |
| Valtchev | Biology    |

# Query 5 - Rename - Alias names for instructor id, name, department and salary

``` SQL
SELECT  id AS instructor_id, name AS instructor_name, dept_name AS  instructor_dept_name , salary as instructor_salary  FROM instructor LIMIT 50;
```

### Output

| instructor_id | instructor_name   | instructor_dept_name | instructor_salary |
|-|-|-|-|
| 14365         | Lembr             | Accounting           |          32241.56 |
| 15347         | Bawa              | Athletics            |          72140.88 |
| 16807         | Yazdi             | Athletics            |          98333.65 |
|........|........|
| 97302         | Bertolino         | Mech. Eng.           |          51647.57 |
| 99052         | Dale              | Cybernetics          |          93348.83 |

# Query 6 - Cartesian Product

# Query 7 - Natrural join

# Query 8 - Theta join

