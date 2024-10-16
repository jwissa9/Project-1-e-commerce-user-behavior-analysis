"""
from mrjob.job import MRJob
from mrjob.step import MRStep

from heapq import nlargest

import csv

class Top3RevenueAnalysis(MRJob):
    
    #adds and argument to allow the products.csv file to be inputted
    #this file will take two files as input
    def configure_args(self):
        super(Top3RevenueAnalysis, self).configure_args()
        self.add_file_arg('--products', help='Path to products.csv')
    
    #this function is needed for the second reducer as it gets the information from products.csv
    #it will help with calculating the revenue and profits
    def load_product_details(self):
        product_details = {} #stores the information in a dictionary
        with open(self.options.products, 'r', encoding='utf-8') as f: #read the products.csv file
            reader = csv.DictReader(f)
            for row in reader:
                product_id = row['ProductID'] #get the id
                product_details[product_id] = { #appends the dictionary that maps the id to its corresponding details
                                               'ProductName': row['ProductName'],
                                               'ProductCategory': row['ProductCategory'], #it needs the category and price data
                                               'Price' : float(row['Price'])
                                               }
        return product_details
    
    #define the steps that will be chained
    def steps(self):
        return [
            MRStep(mapper=self.mapper_revenue, reducer=self.reducer_sum_revenue),
            MRStep(reducer=self.reducer_top3_average)
        ]
    
    #mapper function to get the revenue fields from transactions.csv
    def mapper_revenue(self,_,line):
        #split of the lines by comma
        fields = line.split(',')
        if fields[0] != 'TransactionID': #skip the first line, which is the column titles
            category = fields[2] #get the category
            product_id = fields[3] #get the product id
            revenue = float(fields[5]) #get the revenue
            yield (product_id, category), revenue #emit the key value pair as id and revenue
    
    #reducer function that calculates the total revenue for each product id        
    def reducer_sum_revenue(self, key, revenues):
        total_revenue = sum(revenues)
        yield key, total_revenue #emit the id and their total revenue
    
    
    #def reducer_debug(self, key, total_revenues):
    #    for total_revenue in total_revenues:
    #        yield key, total_revenue

    
    def reducer_top3_average(self,product_key,revenue):
        product_details = self.load_product_details() #get the product details from the products.csv in a dictionary format to do the calculations
        category_revenues = {} #storage for the list of products and revenues for each category
        
        for total_revenue in revenue:
            product_id, category = product_key            
            product_info = product_details.get(product_id, {}) #get the product details by id
            #category = product_info.get('ProductCategory', 'Unknown')
            price = product_info.get('Price', 0)
            profit = total_revenue - price
        
            #start grouping the products by their category
            #if a category doesn't exist, initialize and empty list
            if category not in category_revenues:
                category_revenues[category] = []
            
            #appedning the category information in the tuple for the corresponding category
            category_revenues[category].append((product_id, total_revenue, profit))
            
        for category, products, in category_revenues.items():
            total_revenue = sum([p[1] for p in products])
            average_revenue = total_revenue / len(products) if products else 0 #get the average revenue
            top3 = nlargest(3, products, key=lambda x:x[2]) #nlargest sorts the products by their top 3
            
            yield category, {
                'average_revenue': average_revenue,
                'top3_products': top3
            }
        
        
if __name__ == '__main__':
    Top3RevenueAnalysis.run()
"""


from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class Top3RevenueAnalysis(MRJob):

    #adds and argument to allow the products.csv file to be inputted
    #this file will take two files as input
    def configure_args(self):
        super(Top3RevenueAnalysis, self).configure_args()
        self.add_file_arg('--products', help='Path to products.csv')
        
    #this function is needed for the reducer functions as it gets the information from products.csv
    #it will help with calculating the revenue and profits
    def load_product_details(self):
        product_details = {} #store the details in a dictionary
        with open(self.options.products, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f) #read the products.csv file
            for row in reader:
                product_details[row['ProductID']] = { #each detail will be mapped to the id
                    'ProductName': row['ProductName'],
                    'ProductCategory': row['ProductCategory'],
                    'Price': float(row['Price'])
                }
        return product_details

    #the steps will chain the functions together
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_aggregate), # Step 1 mapper and reducer aggregates the revenues
            MRStep(reducer=self.reducer_avg), #step 2 is to average the revenue
            MRStep(reducer=self.reducer_top) #step 3 is to find the top 3
        ]

    def mapper(self, _, line): #maps the elements
        if 'TransactionID' not in line: #skip the header
            transaction_id, user_id, category, product_id, qty, revenue, timestamp = line.split(',') #extract all the elements by splitting off the line by ','
            yield (product_id, category), (float(revenue), 1) #emit the tuple with key as product id and category and value as revenue

    def reducer_aggregate(self, key, values): #aggregates the total revenue
        total_revenue = 0
        total_count = 0
        for revenue, count in values:
            total_revenue += revenue
            total_count += count
        product_id, category = key
        yield category, (total_revenue, total_count, product_id)
                         
    def reducer_avg(self, category, values): #get the average for each product id
        product_details = self.load_product_details()  #get the product details
        product_revenues = []
        #count_per_product = {}
        
        for total_revenue, total_count, product_id in values:
            
            #if product_id not in count_per_product:
            #    count_per_product[product_id] = 0
            #count_per_product[product_id] += 1
            
            product_info = product_details.get(product_id, {}) #get the corresponding id
            price = product_info.get('Price', 0) #get the price, default if 0
            profit = total_revenue - price #calculate the profit
            avg_revenue = total_revenue / total_count
            product_revenues.append((total_revenue, product_id, avg_revenue, profit)) #append the total, id, and profit in an array
        
        #emits the product information under a category, total, id, average, and profit
        for total_revenue, product_id, avg_revenue, profit in product_revenues:
            #avg_revenue = total_revenue / count_per_product[product_id]
            yield category, (total_revenue, product_id, avg_revenue, profit) #this will be chained to the top3 function to get the top 3 and the total and average for each product

    def reducer_top(self, category, values): #get the top 3 and total and average for each product
        products = list(values) #puts the values to a list for easier extraction

        total_revenue_per_product = [(p[1], p[0]) for p in products] #total revenue per product
        average_revenue_per_product = [(p[1], p[2]) for p in products] #average revenue per product
        top_products = sorted(products, reverse=True, key=lambda x: x[3])[:3] #sort the products by profit and get the highest 3

        yield category, { #emit the results under each category
            'total_revenue_per_product': total_revenue_per_product,
            'average_revenue_per_product': average_revenue_per_product,
            'top3_products': top_products
        }
        
if __name__ == '__main__':
    Top3RevenueAnalysis.run()