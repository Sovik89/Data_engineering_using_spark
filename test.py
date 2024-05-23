#print("Hello World")
department_df=spark.\
    read.\
        csv("/user/sovik/retail_db/departments",
            schema='department_id INT,department_name STRING')