1. Data Structure and Overview
Dimensions & Schema: Check the number of rows/columns and data types (numeric, categorical, datetime) using functions like df.info().
Data Snapshot: Look at the top (head()) and bottom (tail()) rows to detect loading errors or inconsistent formatting.
Variable Types: Classify columns as numerical (continuous/discrete), categorical (nominal/ordinal), or binary to determine appropriate analysis methods. 
2. Data Quality Management
Missing Values: Identify the percentage of missing values per column (.isnull().sum()) and investigate if they are missing at random or systematically.
Duplicates: Identify and remove duplicate records that could skew results.
Consistency Checks: Ensure numerical columns don't have impossible values (e.g., negative age) and check categorical columns for typos or inconsistent labels (e.g., "NY" and "New York"). 
3. Univariate Analysis (Single Variable)
Numerical Features: Calculate summary statistics (mean, median, standard deviation, quartiles) using describe(). Visualize distributions using histograms and box plots to check for skewness and central tendency.
Categorical Features: Use frequency counts (value_counts()) to check for class imbalances and visualize with bar plots.
Outlier Detection: Use box plots to visually identify data points far outside the interquartile range (IQR). 
4. Bivariate & Multivariate Analysis (Relationships) 
Correlation Analysis: Compute a correlation matrix for numerical features and visualize it using a heatmap to identify strong relationships or multicollinearity.
Numerical-Numerical: Create scatter plots to examine relationships between pairs of continuous variables.
Numerical-Categorical: Use side-by-side box plots to compare distributions of a numerical feature across different categories.
Categorical-Categorical: Utilize cross-tabulation or stacked bar charts to analyze the relationship between two categorical variables. 
5. Target Variable Analysis
Target Distribution: If it's a supervised learning task, visualize the target variable to check for balance (classification) or skewness (regression).
Target-Feature Relation: Analyze how individual features relate to the target variable to guide feature selection. 