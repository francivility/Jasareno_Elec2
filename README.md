# LeaderSurname_Elec2

## Lab 2 — Apache Spark Partition Strategies

### Dataset
**Philippine Public Education Data (DepEd)**  
Source: https://www.kaggle.com/datasets/johnjoemmelrodelas/philippine-public-education-data  
File needed: `philippine_public_education.csv`

---

### How to Run

1. Download the dataset from Kaggle and place `philippine_public_education.csv` in the same folder as `Lab2.py`
2. Make sure you have Python and PySpark installed
3. Run the script:
   ```
---

### What the Script Does

| Section | Description |
|---|---|
| Partition Strategy 1 | Repartitions data by `region`, counts schools per region |
| Partition Strategy 2 | Hash partitions into 5 partitions by `division`, counts schools per division |
| Transformation 1 | Filters public schools only |
| Transformation 2 | Sorts schools by total enrollment (highest first) |
| Transformation 3 | Filters schools in NCR only |
| Transformation 4 | Shows total schools, total enrolled, and average enrollment per region |


### Requirements

- Python 3.8+
- Apache Spark 3.x
- PySpark
