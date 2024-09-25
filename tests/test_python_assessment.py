import unittest
import pandas as pd
import sys
sys.path.insert(1, '/Users/bcardoza/CODE/bungee_tech_project/code')
from main import BungeeTechPythonAssignment


class TestBungeeTechProjectPythonAssessment(unittest.TestCase):
    '''
    '''
    @classmethod
    def setUpClass(cls):
        cls.obj = BungeeTechPythonAssignment('/Users/bcardoza/CODE/bungee_tech_project/data')
        #cls.obj.df = cls.df

    def test_q1(self):
        returned_df = self.__class__.obj.run_q1()
        filtered_df = returned_df[returned_df['SKU']==11331]  # Check value for one SKU
        self.assertEqual(round(float(filtered_df['AVERAGE_PRICE'].iloc[0]), 6), 367.546667)

    def test_q2(self):
        returned_df = self.__class__.obj.run_q2()
        filtered_df = returned_df[returned_df['COUNTRY']=='UK']  # Check for UK Country
        self.assertEqual(int(filtered_df['UNIQUE_SKU_COUNT'].iloc[0]), 268)

    def test_q3(self):
        returned_df = self.__class__.obj.run_q3()
        # Check if the 2 new columns are present
        self.assertIn('CURRENCY', list(returned_df.columns))
        self.assertIn('UNIT_OF_MEASURE', list(returned_df.columns))
        


if __name__ == '__main__':
    unittest.main()