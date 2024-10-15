
from mrjob.job import MRJob
from mrjob.step import MRStep

from heapq import nlargest

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
            for line in f:
                fields = line.strip().split(',') #split off the lines
                if fields[0] != 'ProductID': #skip the header
                    product_id = fields[0] #get the id
                    product_details[product_id] = { #appends the dictionary that maps the id to its corresponding details
                        'ProductName': fields[1],
                        'ProductCategory': fields[2], #it needs the category and price data
                        'Price' : float(fields[3])
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
        #product_id, category = key
        yield key, total_revenue #emit the id and their total revenue
        
    def reducer_top3_average(self,product_key,revenue):
        product_details = self.load_product_details() #get the product details from the products.csv in a dictionary format to do the calculations
        category_revenues = {} #storage for the list of products and revenues for each category
        
        
        for product_key, total_revenue in revenue:
            product_id, category = product_key            
            product_info = product_details.get(product_id, {}) #get the product details by id
            category = product_info.get('ProductCategory', 'Unknown')
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
            average_revenue = total_revenue / len(products) #get the average revenue
            top3 = nlargest(3, products, key=lambda x:x[2]) #nlargest sorts the products by their top 3
            
            yield category, {
                'average_revenue': average_revenue,
                'top3_products': top3
            }
        
    
if __name__ == '__main__':
    Top3RevenueAnalysis.run()