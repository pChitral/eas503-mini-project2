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

    # SQL query for creating the Region table
    create_table_sql = """CREATE TABLE [Region] (
    [RegionID] Integer not null primary key,
    [Region] Text not null
    );
    """
    conn = create_connection(normalized_database_filename)

    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    # Creating a function for insertion in our table, which is freshly created above
    def insert_region(conn, values):
        sql = ''' INSERT INTO Region(Region)
                VALUES(?) '''
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid

    # That the insert_region function is ready, we can start putting in values in the table as follows!
    conn_norm = create_connection(normalized_database_filename)
    with conn_norm:
        for region in region_list:
            insert_region(conn_norm, (region, ))

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
    region_to_regionid_dictionary = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    with open(data_filename) as file:
        i = iter(file)
        i.__next__()
        for line in i:
            if [line.split("\t")[3], region_to_regionid_dictionary[line.split("\t")[4]]] not in country_region_list:
                country_region_list.append([line.split("\t")[3], region_to_regionid_dictionary[line.split("\t")[4]]])
            else:
                continue
    country_region_list.sort()        
    # SQL query for creating the Region table
    create_table_sql = """CREATE TABLE [country] (
    [CountryID] integer not null Primary key,
    [Country] Text not null,
    [RegionID] integer not null,
    FOREIGN KEY(RegionID) REFERENCES Region(RegionID)
    );
    
    """
    conn = create_connection(normalized_database_filename)

    # Running the query by passing it to the `create_table` function
    create_table(conn, create_table_sql)

    # Creating a function for insertion in our table, which is freshly created above
    def insert_country_region(conn, values):
        sql = ''' INSERT INTO country(Country, RegionID)
                VALUES(?, ?) '''
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.lastrowid

    # That the insert_country_region function is ready, we can start putting in values in the table as follows!
    conn_norm = create_connection(normalized_database_filename)

    with conn_norm:
        for country_region in country_region_list:
            try:
                insert_country_region(conn_norm, (country_region[0], country_region[1]))
            except Error:
                print(Error)

    # END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    pass

    # END SOLUTION


def step5_create_customer_table(data_filename, normalized_database_filename):

    # BEGIN SOLUTION

    pass

    # END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    pass

    # END SOLUTION


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    # BEGIN SOLUTION
    pass

    # END SOLUTION


def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):

    # BEGIN SOLUTION
    pass

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
