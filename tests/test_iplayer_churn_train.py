from pandas.testing import assert_frame_equal
from src.learn import model_stacking
import datatest as dt

def test_input_dim(df, training_df_light_n_cols):
  assert len(df.columns) == training_df_light_n_cols, "Unexpected number of columns."
  print('Engineered data has the expected number of columns.')

def test_nulls(df):
  dataNulls = df.isnull().sum().sum()
  assert dataNulls == 0, "Nulls in engineered data."
  print('Engineered features do not contain nulls.')