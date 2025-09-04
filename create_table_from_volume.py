### create a file in volume
import json
from datetime import datetime
import random
import pandas as pd

data = [
    {
        "emp_name": "himanshu",
        "course_id": 1,
        "course_completed": "Y",
        "order_id": 11,
        "order_ts": "2025-09-04 10:30:00",
        "age": 24
    },
    {
        "emp_name": "atul",
        "course_id": 1,
        "course_completed": "N",
        "order_id": 21,
        "order_ts": "2025-09-04 10:30:00",
        "age": 23
    },
    {
        "emp_name": "rohit",
        "course_id": 2,
        "course_completed": "Y",
        "order_id": 31,
        "order_ts": "2025-09-04 10:30:00",
        "age": 24
    },
    {
        "emp_name": "rohit",
        "course_id": 6,
        "course_completed": "N",
        "order_id": 71,
        "order_ts": "2025-09-04 10:30:00",
        "age": 24
    },
    {
        "emp_name": "roshan",
        "course_id": 3,
        "course_completed": "N",
        "order_id": None,
        "order_ts": "2025-09-04 10:30:00",
        "age": 26
    },
    {
        "emp_name": "swap",
        "course_id": 4,
        "course_completed": "Y",
        "order_id": 51,
        "order_ts": "2025-09-04 10:30:00",
        "age": 24
    },
    {
        "emp_name": "swap",
        "course_id": 5,
        "course_completed": "Y",
        "order_id": 61,
        "order_ts": "2025-09-04 10:30:00",
        "age": 24
    }
]

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data)

# Save the DataFrame as a Parquet file in the specified volume
df.to_parquet("/Volumes/workspace/default/course2/sample1.parquet", index=False)










### create new file- updated records
import json
from datetime import datetime
import random
import pandas as pd

data = [
    {
        "emp_name": None,
        "course_id": 1,
        "course_completed": "Record_deleted",
        "order_id": None,
        "order_ts": "2025-09-04 10:31:00",
        "age": None
    },
    {
        "emp_name": "rohit",
        "course_id": 2,
        "course_completed": "Y",
        "order_id": 34,
        "order_ts": "2025-09-04 10:31:00",
        "age": 24
    }
]

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data)

# Save the DataFrame as a Parquet file in the specified volume
df.to_parquet("/Volumes/workspace/default/course2/sample2.parquet", index=False)





### copy files in volume
dbutils.fs.cp("/Volumes/workspace/default/course2/sample1.parquet", "/Volumes/workspace/default/course2/sample1_copy.parquet")
