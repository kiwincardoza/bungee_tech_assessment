import pandas as pd
import traceback
import logging


class BungeeTechPythonAssignment:
    '''
    Class for reading Dataframe from CSV file and performing the transformations for the three questions
    '''
    def __init__(self, data_home_path):
        self.data_home_path = data_home_path

        # Load CSV file into Dataframe
        self.df = pd.read_csv(f"{self.data_home_path}/MAIN_DE.csv", 
                              delimiter=',')
        
        self.logger = logging.getLogger(__name__) # Create logger
        logging.basicConfig(
            filename='/Users/bcardoza/CODE/bungee_tech_project/logs/main.log', 
            level=logging.INFO)
    
    def run_q1(self):
        ''' Q1 - Average Price per SKU
        '''
        try:
            self.logger.info(f"Start of run_q1()")
            q1_df = self.df.copy()
            q1_df['PRICE'] = q1_df['PRICE'].apply(lambda val: val.replace('$','').replace(' ',''))  # Preprocess the PRICE column to get the aggregate
            q1_df['PRICE'] = q1_df['PRICE'].astype(float)
            q1_grouped_df = q1_df.groupby("SKU").agg({'PRICE':'mean'}).reset_index()  # Group by on SKU 
            self.logger.info(f"q1_df is grouped")
            q1_final_df = q1_grouped_df.rename(columns={'PRICE':'AVERAGE_PRICE'})   # Rename the aggregated column
            print(q1_final_df)
            self.logger.info(f"End of run_q1()")
            return q1_final_df
        except Exception :
            self.logger.error(traceback.format_exc())

    def run_q2(self):
        ''' Q2 - Country wise Number of unique products  being sold,  descending  order of unique products
        '''
        try:
            self.logger.info(f"Start of run_q2()")
            q2_df = self.df.copy()
            q2_grouped_df = q2_df.groupby('COUNTRY').agg({'SKU': 'nunique'}).reset_index()  # Group by Country and get the unique number of SKUs
            self.logger.info(f"q2_df is grouped")
            q2_ordered_df = q2_grouped_df.sort_values('SKU', ascending=False)  # Sort SKU number in descending order
            self.logger.info(f"q2_df is ordered")
            q2_final_df = q2_ordered_df.rename(columns={'SKU':'UNIQUE_SKU_COUNT'})  # Rename column
            print(q2_final_df)
            self.logger.info(f"End of run_q2()")
            return q2_final_df
        except Exception :
            self.logger.error(traceback.format_exc())

    def run_q3(self):
        ''' Q3 - Convert The given CSV to parquet, with two new columns
        column 1 ::    'currency' ,  populate the currency column with an appropriate value.
        column 2 ::    'unit of measure' ,  populate column with an appropriate value.
        Dtypes for  Price  column should be Float, rest all string
        '''
        try:
            self.logger.info(f"Start of run_q3()")
            q3_df = self.df.copy()
            q3_df['CURRENCY'] = q3_df['PRICE'].apply(lambda val: val[0])  # Get the Currency notation ($) from the PRICE column
            q3_df['UNIT_OF_MEASURE'] = q3_df['CAPACITY'].apply(lambda val: ''.join(filter(str.isalpha, val)))  # Get only the letters from CAPCITY column (L/ml)
            self.logger.info(f"Created new columns")
            q3_df['PRICE'] = q3_df['PRICE'].apply(lambda val: val.replace('$','').replace(' ',''))
            q3_df['PRICE'] = q3_df['PRICE'].astype(float)
            for col in [c for c in list(q3_df.columns) if c!='PRICE']:  # Convert all columns to string except for PRICE column 
                q3_df[col] = q3_df[col].astype(str) 

            self.logger.info(f"Datatypes converted")
            
            print(q3_df.dtypes)
            print(q3_df)

            # Save DF as a Parquet file
            op_file_name = f"{self.data_home_path}/q3_output.parquet"
            q3_df.to_parquet(op_file_name)
            self.logger.info(f"q3_df written to {op_file_name}")
            self.logger.info(f"End of run_q3()")
            return q3_df
        except Exception :
            self.logger.error(traceback.format_exc())
        


if __name__ == '__main__':
    '''
    Main function for calling the Class functions
    '''
    try:
        bungee_tech_assignment_project_obj = BungeeTechPythonAssignment("/Users/bcardoza/CODE/bungee_tech_project/data")
        bungee_tech_assignment_project_obj.run_q1()
        bungee_tech_assignment_project_obj.run_q2()
        bungee_tech_assignment_project_obj.run_q3()
    except Exception:
        print(traceback.format_exc())






