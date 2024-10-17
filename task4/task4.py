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

    #the steps will chain the functions together as mapreduce sequence
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

    def reducer_aggregate(self, key, values): #aggregates the total revenue and count
        total_revenue = 0
        total_count = 0
        for revenue, count in values:
            total_revenue += revenue #acculmulate the revenue
            total_count += count #count the number of product occurrences
        product_id, category = key
        yield category, (total_revenue, total_count, product_id) #emit category as key and the value tuple of total, count, and product id
                         
    def reducer_avg(self, category, values): #get the average for each product id
        product_details = self.load_product_details()  #get the product details
        product_revenues = []
        
        for total_revenue, total_count, product_id in values: #for every total, count, and id
            
            product_info = product_details.get(product_id, {}) #get the corresponding id of the product details
            price = product_info.get('Price', 0) #get the price, 0 if none
            profit = total_revenue - price #calculate the profit
            avg_revenue = total_revenue / total_count #calculate the average
            product_revenues.append((total_revenue, product_id, avg_revenue, profit)) #append the total, id, and profit in an array
        
        #emits the product information under a category with values as total, id, average, and profit
        for total_revenue, product_id, avg_revenue, profit in product_revenues:
            yield category, (total_revenue, product_id, avg_revenue, profit) #this will be chained to the top3 function to get the top 3 and the total and average for each product

    def reducer_top(self, category, values): #get the top 3 and total and average for each product
        products = list(values) #puts the values to a list for easier extraction

        total_revenue_per_product = [(p[1], p[0]) for p in products] #extract total revenue per product
        average_revenue_per_product = [(p[1], p[2]) for p in products] #extract average revenue per product
        top_products = sorted(products, reverse=True, key=lambda x: x[3])[:3] #sort the products by profit and get the highest 3

        yield category, { #emit the results under each category
            'total_revenue_per_product': total_revenue_per_product,
            'average_revenue_per_product': average_revenue_per_product,
            'top3_products': top_products
        }
        
if __name__ == '__main__':
    Top3RevenueAnalysis.run()