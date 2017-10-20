SELECT name, age, job
FROM employees
WHERE (salary >= 120000
AND name = 'David') OR title = 'Sales Associate' OR gender = 'female'
;