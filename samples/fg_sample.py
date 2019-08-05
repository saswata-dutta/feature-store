import os

import numpy as np
import pandas as pd

from featurestore.clients.fg import (
    add_fg_partition,
    create_fg,
    dump_fg,
    get_versions,
    read_fg,
    upload_fg,
)

# if getting error as java exited before sending port, then JAVA_HOME must be unset,
# best to set it in your shell profile : 
# add following in it, if on mac and installed java using brew, 
# get "java -version"
# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8???)
# or set below as ls -al `which java`
# or $(dirname $(readlink $(which javac)))/java_home
# or $(dirname $(which javac))/java_home
# or java -version -XshowSettings:properties
os.environ["JAVA_HOME"] = "~/.sdkman/candidates/java/current"

# if spark cant be initialised error,
# may need to explicitly specify if spark is in custom locations: ls -al `which pyspark`
os.environ["SPARK_HOME"] = "/usr/local/Cellar/apache-spark/2.4.3"

# if driver and executor have incompatible python version error,
# may need this if trying out from ipython repl,
# set it to same python which runs note book : ls -al `which python3`
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"
# specify if offline, and get errors as got mac instead of if
os.environ["SPARK_LOCAL_IP"] = "localhost"

# pandas input
data = [
    [
        "Alex",
        True,
        10,
        10.5,
        np.datetime64("2015-02-25"),
        [4],
        1554295898,
        {"att1": "att1_v_1", "att2": 1},
    ],
    [
        "Bob",
        False,
        12,
        13.1,
        np.datetime64("2005-02-25"),
        [3, 4, 5, 1000],
        1454295898,
        {"att1": "att1_v_2", "att2": 2},
    ],
    [
        "Clarke",
        True,
        13,
        20.45,
        np.datetime64("2001-02-25"),
        [0],
        1354295898,
        {"att1": "att1_v_3", "att2": 3},
    ],
    [
        "Dick",
        False,
        12,
        13.1,
        np.datetime64("2010-02-25"),
        [1, 2],
        1254295898,
        {"att1": "att1_v_4", "att2": 4},
    ],
]

df = pd.DataFrame(
    data, columns=["Name", "Active", "Age", "Score", "B_day", "counts", "ts", "attrs"]
)


success1, response1 = create_fg(
    "business", "user", "activity", "v0001", "ts", time_col_unit="s", pandas_df=df
)
print(success1, response1)

versions = get_versions("business", "user", "activity")
print(versions)

success2, response2 = upload_fg("business", "user", "activity", "v0001", df)
print(success2, response2)

# add partitions to pre created data, instead of upload
success3, response3 = add_fg_partition(
    "business",
    "user",
    "activity",
    "v0001",
    ["y=2009/m=09/d=30", "y=2009/m=09/d=29"],
)
print(success3, response3)

# put in sql query after consulting Glue or response of "create_fg" for table name
success4, query_id, s3path, response4 = dump_fg(
    "select * from feature_store.business_user_activity_v0001;"
)
print(success4, query_id, s3path, response4)

# dont spam this as it incurs AWS Lambda cost
out_df, out_schema = read_fg(query_id)
print(out_schema)
