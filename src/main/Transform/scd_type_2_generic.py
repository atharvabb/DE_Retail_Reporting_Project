from pyspark.sql import DataFrame
from pyspark.sql.functions import isnull, lit, expr


def scd_type_2(oldDataFrame:DataFrame,newDataFrame:DataFrame,keycol_to_compare,scdflag,distinct_col):
    JOIN_TYPE = "full_outer"
    Join_Key = keycol_to_compare
    oldDfColumns = oldDataFrame.columns
    newDfColumns = newDataFrame.columns
    if Join_Key not in oldDfColumns and Join_Key not in newDfColumns:
        raise Exception(f"""
             key = {keycol_to_compare} Does not Present In either of DF's.
             Both Df Should Have column with name {keycol_to_compare} to make scd type 2 work""")

    df1col,df2col=[],[]
    for col in oldDfColumns:
        oldDataFrame=oldDataFrame.withColumnRenamed(col,col+"_df1")
        df1col.append(col+"_df1")
    for col in newDfColumns:
        newDataFrame=newDataFrame.withColumnRenamed(col,col+"_df2")
        df2col.append(col + "_df2")

    JoinedDF = oldDataFrame.join(newDataFrame,newDataFrame[f"{keycol_to_compare}_df2"] == oldDataFrame[f"{keycol_to_compare}_df1"],JOIN_TYPE)

    newDF = JoinedDF.filter(newDataFrame[f"{Join_Key}_df2"].isNotNull() & oldDataFrame[f"{Join_Key}_df1"].isNull())\
            .select(df2col).withColumn(f"{scdflag}_df2",lit(1))

    deletedDF = JoinedDF.filter(newDataFrame[f"{Join_Key}_df2"].isNull() & oldDataFrame[f"{Join_Key}_df1"].isNotNull())\
             .select(df1col).withColumn(f"{scdflag}_df1",lit(0)).drop(f"{scdflag}")

    updatedDF_raw = JoinedDF.filter(newDataFrame[f"{Join_Key}_df2"].isNotNull() & oldDataFrame[f"{Join_Key}_df1"].isNotNull())\
                 .filter(oldDataFrame[f"{distinct_col}_df1"] != newDataFrame[f"{distinct_col}_df2"])
    updatedDFactive =  updatedDF_raw.select(df2col).withColumn(f"{scdflag}_df2",lit(1))
    updatedDFInActive = updatedDF_raw.select(df1col).withColumn(f"{scdflag}_df1",lit(0)).drop(f"{scdflag}")

    unchangedDF = JoinedDF.filter(expr(f"{Join_Key}_df1 IS NOT NULL and {Join_Key}_df2 IS NOT NULL"))\
                 .filter(f"{distinct_col}_df1 = {distinct_col}_df2").select(df1col).drop(f"{scdflag}")

    #Removing custom column suffix
    for i in oldDfColumns:
        newDF= newDF.withColumnRenamed(i+"_df2",i)
        deletedDF=deletedDF.withColumnRenamed(i+"_df1",i)
        unchangedDF=unchangedDF.withColumnRenamed(i+"_df1",i)
        updatedDFactive=updatedDFactive.withColumnRenamed(i+"_df2",i)
        updatedDFInActive=updatedDFInActive.withColumnRenamed(i+"_df1",i)
    #return updatedDFInActive
    return newDF.union(deletedDF).union(unchangedDF).union(updatedDFactive).union(updatedDFInActive)