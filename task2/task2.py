from mrjob.job import MRJob
from mrjob.step import MRStep

class ProductConversionRate(MRJob):
    
    def mapper(self, _, line):
        # Parse each line of the CSV file
        fields = line.split(',')
        
        # Determine which file we are reading by checking the number of fields
        if len(fields) == 5:  # user_activity.csv
            log_id, user_id, activity_type, product_id, activity_timestamp = fields
            # For interactions (browse, add_to_cart)
            if activity_type in ('browse', 'add_to_cart'):
                yield product_id, ('interaction', activity_type)
            # For purchases
            elif activity_type == 'purchase':
                yield product_id, ('purchase', 1)
        elif len(fields) == 7:  # transactions.csv
            transaction_id, user_id, product_category, product_id, quantity_sold, revenue, transaction_timestamp = fields
            # Emit product category along with product ID for purchase lookup
            yield product_id, ('product_category', product_category)
    
    def reducer_join(self, product_id, values):
        # Separate interactions/purchases from product category
        product_category = None
        interactions = 0
        purchases = 0
        
        for value_type, value in values:
            if value_type == 'product_category':
                product_category = value
            elif value_type == 'interaction':
                interactions += 1
            elif value_type == 'purchase':
                purchases += 1
        
        # Only emit if we know the product category
        if product_category is not None:
            yield product_category, (interactions, purchases)
    
    def reducer_calculate_rate(self, product_category, values):
        total_interactions = 0
        total_purchases = 0
        
        for interactions, purchases in values:
            total_interactions += interactions
            total_purchases += purchases
        
        # Calculate conversion rate (if there are any interactions)
        if total_interactions > 0:
            conversion_rate = total_purchases / total_interactions
            yield product_category, conversion_rate

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_join),
            MRStep(reducer=self.reducer_calculate_rate)
        ]

if __name__ == '__main__':
    ProductConversionRate.run()
