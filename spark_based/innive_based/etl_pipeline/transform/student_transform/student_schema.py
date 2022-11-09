
from pyspark.sql.types import StructField,StructType, IntegerType, DateType, StringType

def schema_student_base():

    schema = StructType([
        StructField("Operation" ,StringType(),nullable=False),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("personId", StringType()),
        StructField("sourceSystemDescriptor", StringType()),
        StructField("birthCity", StringType(), nullable=False),
        StructField("birthCountryDescriptor", StringType()),
        StructField("birthDate", StringType(), nullable=True),
        StructField("birthInternationalProvince", StringType()),
        StructField("birthSexDescriptor", StringType()),
        StructField("birthStateAbbreviationDescriptor", StringType()),
        StructField("citizenshipStatusDescriptor", StringType()),
        StructField("dateEnteredUS", StringType()),
        StructField("firstName", StringType()),
        StructField("generationCodeSuffix", StringType()),
        StructField("lastSurname", StringType()),
        StructField("maidenName", StringType()),
        StructField("middleName", StringType()),
        StructField("multipleBirthStatus", StringType()),
        StructField("personalTitlePrefix", StringType()),
        StructField("tx_adultPreviousAttendanceIndicator", StringType()),
        StructField("tx_localStudentID", StringType()),
        StructField("tx_studentID", StringType()),
    ])
    return schema


def schema_student_id():
    """Schema for Student Identification Documents"""
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("identificationDocumentUseDescriptor", StringType()),
        StructField("personalInformationVerificationDescriptor", StringType()),
        StructField("issuerCountryDescriptor", StringType(), nullable=False),
        StructField("documentExpirationDate", StringType()),
        StructField("documentTitle", StringType(), nullable=True),
        StructField("issuerDocumentIdentificationCode", StringType(), nullable=True),
        StructField("issuerName", StringType(), nullable=True),
        ])
    return schema


def schema_student_otherNames():
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("otherNameTypeDescriptor", StringType()),
        StructField("firstName", StringType()),
        StructField("generationCodeSuffix", StringType(), nullable=False),
        StructField("lastSurname", StringType(), nullable=False),
        StructField("middleName", StringType(), nullable=False),
        StructField("personalTitlePrefix", StringType(), nullable=False),
        ])
    return schema


def schema_student_pid():
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("otherNameTypeDescriptor", StringType()),
        StructField("firstName", StringType()),
        StructField("generationCodeSuffix", StringType(), nullable=False),
        StructField("lastSurname", StringType(), nullable=False),
        StructField("middleName", StringType(), nullable=False),
        StructField("personalTitlePrefix", StringType(), nullable=False),
    ])
    return schema


def schema_student_crisisEvents():
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("tx_crisisEventDescriptor", StringType()),
        StructField("tx_beginDate", StringType()),
        StructField("tx_endDate", StringType(), nullable=False),
    ])
    return schema


def schema_student_visas():
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("visaDescriptor", StringType()),
        ])
    return schema


def schema_student_scbg():
    schema = StructType([
        StructField("Operation", StringType()),
        StructField("studentUniqueId", StringType(), nullable=False),
        StructField("tx_studentCensusBlockGroup", StringType()),
        StructField("tx_beginDate", StringType()),
        StructField("tx_endDate", StringType()),
        ])
    return schema




