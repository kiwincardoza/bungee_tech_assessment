/*
SAMPLE VALUES USED FOR TESTING THIS QUERY

INSERT INTO EMPLOYEES VALUES
(1, 'Benny Cardoza', 10),
(2, 'Virender Cardoza', 10),
(3, 'Karanam Cardoza', 20),
(4, 'Dinesh Manivannan', 10)

INSERT INTO EMPLOYEES VALUES
(5, 'Philip Manivannan', 30)
*/
WITH LAST_NAME_ADDITION AS (  -- Get the Last name for each name (by splitting the name string col and getting the last element in the array)
    SELECT *,  
    GET(SPLIT(NAME, ' '), ARRAY_SIZE(SPLIT(NAME, ' '))-1) AS LAST_NAME  -- ARRAY_SIZE is used to get the last index of the array
    FROM EMPLOYEES
),
DUPLICATE_LAST_NAMES AS (  -- Main CTE to map the Employees with same last name but different Departments
    SELECT ARRAY_TO_STRING(ARRAY_SORT(ARRAY_CONSTRUCT(A.ID, B.ID)), '') AS SORTED_DUP_ARRAY,  -- Constructing a string of ordered numbers (IDs) (to eliminate duplicate combinations)
    A.NAME AS EMPLOYEE1_NAME, A.DEPARTMENT_ID AS DEPARTMENT1_NAME,
    B.NAME AS EMPLOYEE2_NAME, B.DEPARTMENT_ID AS DEPARTMENT2_NAME
    FROM LAST_NAME_ADDITION A
    JOIN LAST_NAME_ADDITION B   -- Self Join
    ON A.LAST_NAME = B.LAST_NAME
    WHERE A.DEPARTMENT_ID <> B.DEPARTMENT_ID
    AND A.ID <> B.ID
)
SELECT * EXCLUDE(SORTED_DUP_ARRAY)
FROM DUPLICATE_LAST_NAMES
QUALIFY ROW_NUMBER() OVER(PARTITION BY SORTED_DUP_ARRAY ORDER BY SORTED_DUP_ARRAY)=1;  -- To actually filter out duplicate name combinations