from pyspark.sql.functions import trim
import pyspark
from pyspark.sql.functions import *
from spark_based.innive_based.etl_pipeline.transform.student_transform.student_schema import *
from spark_based.innive_based.etl_pipeline.transform.common_pyspark_functions import *

import spark_based.innive_based.etl_pipeline.logger as logger
logger = logger.YarnLogger()
logger.info("Log working in student_code")



def transform_student_base(dir_path = None, filepath="Student_Base.csv", created_schema=None):
    """transform student base data"""
    df_base = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    logger.info(f"{df_base.columns}")
    df_base = trim_columns(df_base)

    # columns_with_value = drop_null_columns(df_base, min_none=0) # check for null values here too
    df_base = df_base.withColumn("personReference", to_json(struct("personId", "sourceSystemDescriptor"))).drop(
        "personId", "sourceSystemDescriptor")
    return df_base


def transform_student_IdDocs(dir_path = None,filepath="Student_identificationDocuments.csv", created_schema=None):
    """transforms student Identification Documents data"""
    df_studentIdDocs = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_studentIdDocs = trim_columns(df_studentIdDocs)
    columns_with_value = drop_null_columns(df_studentIdDocs, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]

    if len(columns_with_value) > 0:
        df_studentIdDocs = df_studentIdDocs.withColumn("personalIdentificationDocuments",
                                                       array(to_json(struct(columns_with_value))))
        logger.info(f"{df_studentIdDocs.columns}")
        return df_studentIdDocs
    else:
        return None


def transform_student_otherNames(dir_path = None, filepath="Student_otherNames.csv", created_schema=None):
    """transforms Student Other Names data"""
    df_otherNames = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_otherNames = trim_columns(df_otherNames)
    columns_with_value = drop_null_columns(df_otherNames, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
    logger.info(f"{df_otherNames.columns}")
    if len(columns_with_value) > 0:
        df_otherNames = df_otherNames \
            .withColumn("otherNames", array(to_json(struct(
            columns_with_value
        ))))
        return df_otherNames
    else:
        return None


def transform_student_pid(dir_path = None, filepath="Student_personalidentificationDocuments.csv", created_schema=None):
    """transforms student personal identification documents data"""
    df_pid = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_pid = trim_columns(df_pid)
    columns_with_value = drop_null_columns(df_pid, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]

    if len(columns_with_value) > 0:
        df_pid = df_pid \
            .withColumn("personalIdentificationDocuments", array(to_json(struct(columns_with_value))))

        return df_pid
    else:
        return None


def transform_scbg(dir_path = None, filepath="Student_studentCensusBlockGroups.csv", created_schema=None):
    """transforms student census block groups data"""

    df_scbg = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_scbg = trim_columns(df_scbg)
    columns_with_value = drop_null_columns(df_scbg, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
    if len(columns_with_value) > 0:
        df_scbg = df_scbg \
            .withColumn("personalIdentificationDocuments", array(to_json(struct(columns_with_value))))

        return df_scbg
    else:
        return None


def transform_crisisEvents(dir_path = None, filepath="Student_CrisisEvents.csv", created_schema=None):
    df_crisisEvents = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_crisisEvents = trim_columns(df_crisisEvents)
    columns_with_value = drop_null_columns(df_crisisEvents, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
    if len(columns_with_value) > 0:
        df_crisisEvents = df_crisisEvents \
            .withColumn("personalIdentificationDocuments", array(to_json(struct(columns_with_value))))

        return df_crisisEvents
    else:
        return None


def transform_student_visa(dir_path = None, filepath="Student_visas.csv", created_schema=schema_student_visas()):
    """transforms student visa data"""
    df_visa = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
    df_visa = trim_columns(df_visa)
    columns_with_value = drop_null_columns(df_visa, min_none=0)
    columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
    if len(columns_with_value) > 0:
        df_visa = df_visa.withColumn("visas", array(to_json(struct(columns_with_value))))
        return df_visa
    else:
        return None


def get_joined_student_data(dir_path):
    """joins all student related data and creates json payload"""
    primary_key = ["studentUniqueId", "Operation"]

    df_base = transform_student_base(dir_path = dir_path,filepath="Student_Base.csv", created_schema=schema_student_base())
    df_visa = transform_student_visa(dir_path = dir_path,filepath="Student_visas.csv", created_schema=schema_student_visas())
    df_pid = transform_student_pid(dir_path = dir_path,filepath="Student_personalidentificationDocuments.csv",
                                   created_schema=schema_student_pid())
    df_otherNames = transform_student_otherNames(dir_path = dir_path,filepath="Student_otherNames.csv",
                                                 created_schema=schema_student_otherNames())
    df_crisisEvents = transform_crisisEvents(dir_path = dir_path,filepath="Student_CrisisEvents.csv",
                                             created_schema=schema_student_crisisEvents())
    df_scbg = transform_scbg(dir_path = dir_path, filepath="Student_studentCensusBlockGroups.csv", created_schema=None)

    dfs = [df_base, df_visa, df_pid, df_otherNames, df_crisisEvents, df_scbg]
    dfs = [x for x in dfs if x is not None]
    df_student = unionAll(dfs=dfs, primary_key=primary_key)

    df_student = df_student.withColumn('payload', to_json(
        struct(
            [str(c) for c in df_student.columns
             if c not in {'Operation', 'visaDescriptor', 'firstName',
                          'generationCodeSuffix', 'lastSurname',
                          'personalTitlePrefix',
                          'middleName', 'otherNameTypeDescriptor'}])))

    df_student = df_student.select(df_student.studentUniqueId, df_student.Operation, df_student.payload)

    return df_student

