# Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):

    if drop_table_name:  # You can optionally pass drop_table_name to drop the table.
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION

    # In the following code I have prepared the data that needs to be inserted in a table
    # by traversing in the data.csv file
    region_list = []
    with open(data_filename) as file:
        i = iter(file)
        i.__next__()
        for line in i:
            if line.split("\t")[4] not in region_list:
                region_list.append(line.split("\t")[4])
            else:
                continue
    region_list.sort()

    # Converting our list of strings into a list of tuples by also adding in the id field
    for i in range(len(region_list)):
        region_list[i] = (i+1, region_list[i])

    """
    
    Setting up connection with the database and creating the table
    
    """
    conn = create_connection(normalized_database_filename)

    # SQL query for creating the Region table
    create_table_sql = """CREATE TABLE [Region] (
    [RegionID] Integer not null primary key,
    [Region] Text not null
    );
    """

    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    """
    
    Dumping in the entire list, all at once, with the help of executemany
    
    """

    c = conn.cursor()
    c.executemany('INSERT INTO Region VALUES(?, ?);', region_list)
    conn.commit()
    conn.close()

    # END SOLUTION


def step2_create_region_to_regionid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    # Fetching all the Regions from our Region table
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT Region, RegionID from Region"
    regions_from_table = execute_sql_statement(sql_statement, conn)

    region_to_regionid_dictionary = {}

    for i in range(len(regions_from_table)):
        region_to_regionid_dictionary[regions_from_table[i]
                                      [0]] = regions_from_table[i][1]

    return region_to_regionid_dictionary

    # END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION

    # In the following code I have prepared the data that needs to be inserted in a table
    # by traversing in the data.csv file
    country_region_list = []

    region_to_regionid_dictionary = step2_create_region_to_regionid_dictionary(
        normalized_database_filename)

    with open(data_filename) as file:
        i = iter(file)
        i.__next__()
        for line in i:
            if [line.split("\t")[3], region_to_regionid_dictionary[line.split("\t")[4]]] not in country_region_list:
                country_region_list.append(
                    [line.split("\t")[3], region_to_regionid_dictionary[line.split("\t")[4]]])
            else:
                continue
    country_region_list.sort()

    # Creating a list of tuples
    for i, country in enumerate(country_region_list):
        country_region_list[i] = (
            i+1, country[0], country[1])

    # SQL query for creating the Region table

    conn = create_connection(normalized_database_filename)

    create_table_sql = """CREATE TABLE [country] (
    [CountryID] integer not null Primary key,
    [Country] Text not null,
    [RegionID] integer not null,
    FOREIGN KEY(RegionID) REFERENCES Region(RegionID)
    );
    """
    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    # I N S E R T I O N   P A R T
    c = conn.cursor()
    c.executemany('INSERT INTO country VALUES(?, ?, ?);', country_region_list)
    conn.commit()
    conn.close()

    # END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT Country, CountryID from country"
    countries_from_table = execute_sql_statement(sql_statement, conn)

    country_to_countryid_dictionary = {}

    for i in range(len(countries_from_table)):
        country_to_countryid_dictionary[countries_from_table[i]
                                        [0]] = countries_from_table[i][1]

    return country_to_countryid_dictionary

    # END SOLUTION


def step5_create_customer_table(data_filename, normalized_database_filename):

    # BEGIN SOLUTION

    country_to_countryid_dictionary = step4_create_country_to_countryid_dictionary(
        normalized_database_filename)
    country_to_countryid_dictionary

    step5_data = []

    with open("data.csv") as file:
        i = iter(file)
        i.__next__()
        for line in i:
            firstName, lastName = line.split("\t")[0].split(" ", 1)
            if [firstName, lastName,  line.split("\t")[1], line.split("\t")[2], country_to_countryid_dictionary[line.split("\t")[3]]] not in step5_data:
                step5_data.append(
                    [firstName, lastName,  line.split("\t")[1], line.split("\t")[2], country_to_countryid_dictionary[line.split("\t")[3]]])
            else:
                continue
    step5_data.sort()
    for i, stuff in enumerate(step5_data):
        step5_data[i] = (i+1, stuff[0], stuff[1], stuff[2], stuff[3], stuff[4])

    # Now here we have prepared the sauce that needs to be cooked

    conn = create_connection(normalized_database_filename)

    create_table_sql = """CREATE TABLE [Customer] (
    [CustomerID] integer not null Primary Key,
    [FirstName] Text not null,
    [LastName] Text not null,
    [Address] Text not null,
    [City] Text not null,
    [CountryID] integer not null,
    foreign key(CountryID) REFERENCES country(CountryID)
    );
    """
    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    # I N S E R T I O N   P A R T
    c = conn.cursor()
    c.executemany('INSERT INTO Customer VALUES(?, ?, ?, ?, ?, ?);', step5_data)
    conn.commit()
    conn.close()

    # END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT FirstName, LastName from Customer"
    customers_from_table = execute_sql_statement(sql_statement, conn)

    customer_to_customerid_dictionary = {}
    customers_from_table[0]

    for i in range(len(customers_from_table)):
        key = str(customers_from_table[i][0]) + \
            " " + str(customers_from_table[i][1])
        customer_to_customerid_dictionary[key] = i + 1

    return customer_to_customerid_dictionary

    # END SOLUTION


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION
    prod_cat_list = []
    with open(data_filename) as file:
        i = iter(file)
        i.__next__()
        for line in i:
            if [line.split("\t")[6].split(",")[0], line.split("\t")[7].split("/")[0]] not in prod_cat_list:
                prod_cat_list.append([line.split("\t")[6].split(
                    ",")[0], line.split("\t")[7].split("/")[0]])
            else:
                continue

    newMaal = []
    for i in range(len(prod_cat_list)):
        if [prod_cat_list[i][0].split(";")[0], prod_cat_list[i][1].split(";")[0]] not in newMaal:
            newMaal.append([prod_cat_list[i][0].split(";")[0],
                           prod_cat_list[i][1].split(";", 1)[0]])
    newMaal.sort()

    for i in range(len(newMaal)):
        newMaal[i] = (i+1, newMaal[i][0], newMaal[i][1])
    newMaal

    # SQL query for creating the Region table

    conn = create_connection(normalized_database_filename)

    create_table_sql = """CREATE TABLE [ProductCategory] (
    [ProductCategoryID] integer not null Primary Key,
    [ProductCategory] Text not null,
    [ProductCategoryDescription] Text not null
    );
    """
    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    """
    
    Dumping in the entire list, all at once, with the help of executemany
    
    """

    c = conn.cursor()
    c.executemany('INSERT INTO ProductCategory VALUES(?, ?, ?);', newMaal)
    conn.commit()
    conn.close()

    # END SOLUTION


def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement = "SELECT ProductCategory, ProductCategoryID from ProductCategory"
    prod_cat_from_table = execute_sql_statement(sql_statement, conn)

    productcategory_to_productcategoryid_dictionary = {}

    for i in range(len(prod_cat_from_table)):
        productcategory_to_productcategoryid_dictionary[prod_cat_from_table[i]
                                                        [0]] = prod_cat_from_table[i][1]
    return productcategory_to_productcategoryid_dictionary

    # END SOLUTION


def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION

    pass

    # END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    pass

    # END SOLUTION


def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION
    pass
    # END SOLUTION


def ex1(conn, CustomerName):

    # Simply, you are fetching all the rows for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID

    # BEGIN SOLUTION
    sql_statement = """
    
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex2(conn, CustomerName):

    # Simply, you are summing the total for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID

    # BEGIN SOLUTION
    sql_statement = """
    
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex3(conn):

    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    # BEGIN SOLUTION
    sql_statement = """
    
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex4(conn):

    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and
    # Region tables.
    # Pull out the following columns.
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    # BEGIN SOLUTION

    sql_statement = """
    
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex5(conn):

    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns.
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending
    # BEGIN SOLUTION

    sql_statement = """

    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):

    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # BEGIN SOLUTION

    sql_statement = """
     
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex7(conn):

   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    # BEGIN SOLUTION

    sql_statement = """
      
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex8(conn):

    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # BEGIN SOLUTION

    sql_statement = """
       
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex9(conn):

    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    # BEGIN SOLUTION

    sql_statement = """
    
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex10(conn):

    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # BEGIN SOLUTION

    sql_statement = """
      
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex11(conn):

    # Find the MaxDaysWithoutOrder for each customer
    # Output Columns:
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate,
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    # BEGIN SOLUTION

    sql_statement = """
     
    """
    # END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
