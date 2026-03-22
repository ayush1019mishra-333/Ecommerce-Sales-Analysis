# =================
# All the Imports 
# =================

import pandas as pd
import matplotlib.pyplot as plt 
import seaborn as sns 
import mysql.connector
import numpy as np

# ===================
# Connections 
# ===================

db1 = mysql.connector.connect( host = "localhost",
                              user = "root",
                              password = "Amit@1019",
                              database = "ecommerce")

cur = db1.cursor()

# ======================
# List all unique cities where customers are located.
# ======================

def get_customer_cities(cur):
    query = "SELECT DISTINCT upper(customer_city) FROM customers"
    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ1: Unique Cities\n")
    df = pd.DataFrame(data,columns = ['City'])

    print(df.head())
 
 
# ==========================
# 2. Count the number of orders placed in 2017. 
# ==========================

def get_ordersfor2017(cur):
    query = "SELECT count(order_id) from orders where year(order_purchase_timestamp) = 2017"

    cur.execute(query)
    data = cur.fetchall()

    print("\nQ2: Orders in 2017\n")
    print(data[0][0])
 
 
# ==========================
# 3. Find the total sales per category.
# ==========================

def get_total_sales(cur):
    query = """SELECT upper(p.product_category) as category , round(sum(py.payment_value),2) as total_sales 
           from products p join order_items o on p.product_id = o.product_id
           join payments py on o.order_id = py.order_id 
           group by p.product_category """
         

    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ3:Total_sales for the individual products\n")
    df = pd.DataFrame(data, columns=['Category', 'Total_Sales'])

    print(df)


# =================================
# 4. Calculate the percentage of orders that were paid in installments.
# =================================

def get_ordersin_installments(cur):
    query = """Select round(sum(case 
               when payment_installments >= 1 then 1 else 0 end )/count(*) *100,2)
               from payments"""
               
    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ4:Percentage of orders made with installments\n")
    print(data[0][0],"%")
    
    
    
# ======================================   
# 5. Count the number of customers from each state. 
# ======================================            


def get_customers_states(cur):
    query = """Select customer_state , count(customer_id) as Count 
               From customers 
               Group by customer_state"""
               
    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ5:Count of customers from each state\n")
    df = pd.DataFrame(data, columns=['State', 'Total'])

    print(df)
    
    plt.figure(figsize=(8,3))
    plt.xlabel("State")
    plt.ylabel("Total customers")
    plt.title("Count of customers from each state")
    plt.bar(df["State"] , df["Total"])
    plt.xticks(rotation=90)
    plt.show()
  
  
  
# INTERMEDIATE QUERIES

# =============================
# 1. Calculate the number of orders per month in 2018.
# =============================

def get_order_per_month(cur):
    query = """SELECT monthname(order_purchase_timestamp) as buy_month , 
               count(order_id) 
               from orders
               where year(order_purchase_timestamp) = 2018
               group by buy_month
               order by buy_month"""
               
    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ1:Count of orders per month in the year 2018\n")

    df = pd.DataFrame(data, columns=['Month','Total_orders'])

    print(df)
    
    fig, ax = plt.subplots(figsize=(8,4))
    bars = ax.bar(df["Month"], df["Total_orders"])
    ax.bar_label(bars, padding=3)
    ax.set_xlabel("Month")
    ax.set_ylabel("Total Orders")
    ax.set_title("Orders per Month (2018)")
    plt.xticks(rotation = 45)
    plt.show()


# ===============================
# 2. Find the average number of products per order, grouped by customer city.
# ===============================

def get_average_of_products(cur):
    
    query = """SELECT upper(c.customer_city) , round(Avg(order_product_count),3) as Average
           from (  SELECT o1.order_id,
           o1.customer_id,
           COUNT(o.product_id) AS order_product_count
           FROM orders o1
           JOIN order_items o
           ON o1.order_id = o.order_id
           GROUP BY o1.order_id, o1.customer_id
           ) t
          join customers c
          on t.customer_id = c.customer_id
          group by c.customer_city
          order by Average desc"""

    cur.execute(query)
    data = cur.fetchall()
    
    print("\nQ2:Average of products per order , grouped by customer city\n")

    df = pd.DataFrame(data, columns=['City','Average'])

    print(df.head())
    
    
# =========================================    
# 3. Calculate the percentage of total revenue contributed by each product category.
# =========================================


def get_percentage_total_revenue(cur):
    query = """SELECT upper(pr.product_category),
       round(SUM(p.payment_value)/ SUM(SUM(p.payment_value)) OVER ()*100,2) AS ratio
       FROM orders o
       JOIN payments p
       ON o.order_id = p.order_id
       JOIN order_items oi
       ON o.order_id = oi.order_id
       JOIN products pr
       ON oi.product_id = pr.product_id
       GROUP BY pr.product_category """
       
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ3:Percentage of Total revenue byb each product category\n")

    df = pd.DataFrame(data, columns=['Products','Percenatage(%)'])

    print(df.head(10))
    
    
# ========================================
# 4. Identify the correlation between product price and the number of times a product has been purchased.
# ========================================    


def get_correlation(cur):
    query = """SELECT upper(p.product_category) , count(o.product_id) as count , round(avg(o.price),2)
               from products p join order_items o 
               on p.product_id = o.product_id 
               group by p.product_category order by count desc"""

   
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ4:Correlation between product price and product bought count\n")

    df = pd.DataFrame(data, columns=['Products','Count','AvgPrice'])

    print(df.head(10))
    arr1 = df["Count"]
    arr2 = df["AvgPrice"]
    val = np.corrcoef(arr1,arr2)
    print("\nThe correlation between them is represented below :")
    print(" ", val[0][1], " ")


# ========================================
# 5. Calculate the total revenue generated by each seller, and rank them by revenue.
# ========================================

def get_totalrevenue_bysellers(cur):
    query = """Select * , dense_rank() over (order by revenue desc) as rnk from
              (SELECT o.seller_id , round(sum(p.payment_value),2) as revenue
              from order_items o 
              join payments p
              on o.order_id = p.order_id
              group by o.seller_id) as a"""
              
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ5:Total revenue generated by each seller and their ranks\n")

    df = pd.DataFrame(data, columns=['Seller_id','Revenue(Rs)','Rank'])
    df = df.head()
    print(df) 
    sns.barplot(x ="Seller_id" , y ="Revenue(Rs)" , data = df)   
    plt.title("Total revenue generated by each seller and their ranks") 
    plt.xticks(rotation=45)
    plt.show()     


# Advance Queries :

# ========================================
# 1. Calculate the moving average of order values for each customer over their order history.
# ========================================

def get_moving_average(cur):
    query = """Select customer_id , order_purchase_timestamp , payment , round(avg(payment) over (partition by customer_id order by order_purchase_timestamp 
               rows between 2 preceding and current row),2) as mov_avg from
               (select o.customer_id , o.order_purchase_timestamp , p.payment_value as payment
               from payments p
               join orders o 
               on p.order_id = o.order_id) as a;"""
               
               
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ1:Moving average of order values for each customer over their order history\n")
    
    df = pd.DataFrame(data, columns=['Customer Id','Timestamp','Payment value' , 'Average value'])
    df = df.head()
    print(df) 



# ========================================
# 2. Calculate the cumulative sales per month for each year.
# ========================================

def get_cummulative_sales(cur):
    query = """SELECT year_period , month_period,
               SUM(monthly_sales) OVER (partition by year_period ORDER BY month_period ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
               FROM (
               SELECT year(o.order_purchase_timestamp) as year_period,
               MONTH(o.order_purchase_timestamp) AS month_period,
               round(SUM(o1.price),2) AS monthly_sales
               FROM orders o
               JOIN order_items o1
               ON o.order_id = o1.order_id
               GROUP BY year(o.order_purchase_timestamp),
               MONTH(o.order_purchase_timestamp)) t"""


    cur.execute(query)
    data = cur.fetchall()

    print("\nQ2:Cumulative sales per month for each year\n")
    
    df = pd.DataFrame(data, columns=['Year','Month','Cummulative Sales'])
    df = df.head(10)
    print(df) 


# ========================================
# 3. Calculate the year-over-year growth rate of total sales.
# ========================================

def get_yoy_growth(cur):
    
    query = """Select year_period , round((payment - lag(payment,1) over (order by year_period)) / lag(payment,1) over (order by year_period)*100,2) as yoy_Sales
               from (SELECT year(o.order_purchase_timestamp) as year_period,
               round(SUM(p.payment_value),2) AS payment
               FROM orders o
               JOIN payments p
               ON o.order_id = p.order_id
               GROUP BY year(o.order_purchase_timestamp)
               order by year_period) t"""
               
               
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ3:The year-over-year growth rate of total sales\n")
    
    df = pd.DataFrame(data, columns=['Year','YOY Sales(%)'])
    print(df)           
    
    plt.plot(df['Year'], df['YOY Sales(%)'], marker='o')
    plt.xlabel("Year")
    plt.ylabel("YOY Sales (%)")
    plt.title("Year-over-Year Sales Growth")
    plt.grid()

    plt.show()


# ========================================
# 4. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.
# ========================================

def get_retention_rate(cur):
    
    query = """SELECT ROUND(COUNT(DISTINCT r.customer_id) * 100.0 / COUNT(DISTINCT f.customer_id),
               2) AS retention_rate
               FROM (   
               -- First purchase per customer                                   
               SELECT customer_id,
               MIN(order_purchase_timestamp) AS first_purchase_date
               FROM orders
               GROUP BY customer_id
               ) f
              LEFT JOIN (
              -- Customers who returned within 6 months
               SELECT DISTINCT o.customer_id
               FROM orders o
               JOIN (
               SELECT customer_id,
               MIN(order_purchase_timestamp) AS first_purchase_date
               FROM orders
               GROUP BY customer_id
               ) f2
               ON o.customer_id = f2.customer_id
               WHERE o.order_purchase_timestamp > f2.first_purchase_date
               AND o.order_purchase_timestamp <= DATE_ADD(f2.first_purchase_date, INTERVAL 6 MONTH)) r
               ON f.customer_id = r.customer_id;"""
               
               
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ4:The retention rate of customers on an interval of 6 months\n")
    
    df = pd.DataFrame(data, columns=['Retention Rate'])
    print(df)        
               

# ========================================
# 5. Identify the top 3 customers who spent the most money in each year.
# ========================================    

def get_top3(cur):
    query = """Select * from 
            (Select year(o.order_purchase_timestamp) as years, o.customer_id , sum(p.payment_value) as payment , rank() over 
            (partition by year(o.order_purchase_timestamp) order by sum(p.payment_value) desc) as rnk
            from orders o
            join payments p
            on o.order_id = p.order_id 
            group by years , customer_id )t
            where rnk <= 3;"""
            
            
               
    cur.execute(query)
    data = cur.fetchall()

    print("\nQ5:The top 3 customers who spent the most money in each year\n")
    
    df = pd.DataFrame(data, columns=['Year' , 'Customer Id' , 'Amount' , 'Rank'])
    print(df)        
    
    sns.barplot( y="Customer Id", x="Amount", hue="Year", data=df ,palette = ["#4C72B0", "#55A868", "#C44E52"])
    plt.title("Top Customers Comparison")
    plt.show()
               
    


# =============================
# Run all the questions
# =============================

get_customer_cities(cur)                 # Easy queries 
get_ordersfor2017(cur)
get_total_sales(cur)
get_ordersin_installments(cur)
get_customers_states(cur)
get_order_per_month(cur)                 # Intermediate Queries
get_average_of_products(cur)
get_percentage_total_revenue(cur)
get_correlation(cur)
get_totalrevenue_bysellers(cur)
get_moving_average(cur)                  # Advance Queries
get_cummulative_sales(cur)
get_yoy_growth(cur)
get_retention_rate(cur)
get_top3(cur)



# ==============
# CLOSING PART 
# ==============

cur.close()
db1.close()