create table departments(
    department_id INT,
    department_name STRING
) USING CSV
OPTIONS(
    sep=",",
    path="/user/sovik/retail_db/departments/"
);

