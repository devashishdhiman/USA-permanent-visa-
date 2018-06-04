# Databricks notebook source
usvisa = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load("/FileStore/tables/us_perm_visas.csv")
usvisa.registerTempTable("usvisa")
usvisa.printSchema()

# COMMAND ----------

#Case Status
usvisa.select("case_status").distinct().show()

# COMMAND ----------

#Maximum Wage per Acceptance
max_wage=sqlContext.sql("Select MAX(INT(wage_offer_from_9089)) from usvisa where case_status='Certified'")
max_wage.show()

# COMMAND ----------

#Minimum Wage per Acceptance
min_wage=sqlContext.sql("Select MIN(INT(wage_offer_from_9089)) from usvisa where case_status='Certified'")
min_wage.show()

# COMMAND ----------

#Acceptance per Country 
acceptance=sqlContext.sql("Select Count(case_status) as acceptance_count ,country_of_citzenship from usvisa where case_status='Certified' group by country_of_citzenship order by acceptance_count DESC")
acceptance.show(10)

# COMMAND ----------

#Rejection per Country
rejection=sqlContext.sql("Select Count(case_status) as rejection_count ,country_of_citzenship from usvisa where case_status='Denied' group by country_of_citzenship order by rejection_count DESC")
rejection.show(10)

# COMMAND ----------

#Application per Country
application=sqlContext.sql("Select Count(case_status) as application_count ,country_of_citzenship from usvisa  group by country_of_citzenship order by application_count DESC")
application.show(10)

# COMMAND ----------

#Acceptance Ratio per Country
import pyspark.sql.functions as F

acceptance_ratio = application\
    .join(acceptance, "country_of_citzenship")\
    .withColumn("ratio", (F.col("acceptance_count") / F.col("application_count")))
    

acceptance_ratio.sort('ratio', ascending=False)
acceptance_ratio.sort('ratio', ascending=False).filter(F.col("application_count") > 100).show(10)

# COMMAND ----------

#Rejection Ratio per country
import pyspark.sql.functions as F

rejection_ratio = application\
    .join(rejection, "country_of_citzenship")\
    .withColumn("ratio", (F.col("rejection_count") / F.col("application_count")))
    
rejection_ratio.sort('ratio', ascending=False)
rejection_ratio.sort('ratio', ascending=False).filter(F.col("application_count") > 100).show(10)

# COMMAND ----------

#Applications Count per company
top_10_applications=sqlContext.sql("Select COUNT(case_number) as app_count,employer_name from usvisa group by employer_name order by app_count DESC")
top_10_applications.show(10)

# COMMAND ----------

#Acceptance count per company
top_10_acceptances=sqlContext.sql("Select COUNT(case_number) as acc_count,employer_name from usvisa where case_status='Certified' group by employer_name order by acc_count DESC ")
top_10_acceptances.show(10)

# COMMAND ----------

#Rejection count per company
top_10_rejections=sqlContext.sql("Select COUNT(case_number) as rej_count,employer_name from usvisa where case_status='Denied' group by employer_name order by rej_count DESC ")
top_10_rejections.show(10)

# COMMAND ----------

#Company Acceptance Ratio
Company_acceptance_ratio = top_10_applications\
    .join(top_10_acceptances, "employer_name")\
    .withColumn("ratio", (F.col("acc_count") / F.col("app_count")))
    
Company_acceptance_ratio.sort('ratio', ascending=False)
Company_acceptance_ratio.sort('ratio', ascending=False).filter(F.col("app_count") > 100).show(20)

# COMMAND ----------

#Year of Application
date= sqlContext.sql("select distinct(YEAR ( decision_date ) )from usvisa")

# COMMAND ----------

#Accepted rate over years
rateaccpt= sqlContext.sql("Select YEAR( decision_date) as Year, count(case_status) as Acceptance_Count  from usvisa where case_status='Certified' group by Year order by  Acceptance_Count").show(10)

# COMMAND ----------

#Rejected rate over years
ratereject= sqlContext.sql("Select  YEAR( decision_date) as Year, count(case_status) as Rejection_Count from usvisa where case_status='Denied' group by Year order by  Rejection_Count").show(10)

# COMMAND ----------

#Training months
exp= sqlContext.sql("select distinct(job_info_training_num_months) from usvisa where job_info_training_num_months !=''")

# COMMAND ----------

#Acceptance
WorkExpAccept= sqlContext.sql("Select job_info_training_num_months, count(case_status) as Acceptance_Count  from usvisa where case_status='Certified' and job_info_training_num_months!='' group by job_info_training_num_months order by  Acceptance_Count DESC").show(10)

# COMMAND ----------

#Rejected
WorkExpReject= sqlContext.sql("Select job_info_training_num_months, count(case_status) as Rejection_Count  from usvisa where case_status='Denied' and job_info_training_num_months!='' group by job_info_training_num_months order by  Rejection_Count DESC ").show(10)

# COMMAND ----------

#Applications
career_application_rate = sqlContext.sql("Select pw_soc_title as career, count(case_status) as app_Count  from usvisa  group by career order by  app_Count DESC")
career_application_rate.show(10)

# COMMAND ----------

#Acceptance rate
career_acceptance_rate= sqlContext.sql("Select pw_soc_title as career, count(case_status) as acc_count  from usvisa where case_status='Certified' group by career order by  acc_count DESC")
career_acceptance_rate.show(10)

# COMMAND ----------

#Acceptance ratio
career_acceptance_ratio = career_application_rate\
    .join(career_acceptance_rate, "career")\
    .withColumn("ratio", (F.col("acc_count") / F.col("app_count")))
    
career_acceptance_ratio.sort('ratio', ascending=False)
career_acceptance_ratio.sort('ratio', ascending=False).filter(F.col("app_count") > 100).show(10)

# COMMAND ----------

#Applications
City= sqlContext.sql("select distinct(job_info_work_city) from usvisa")

# COMMAND ----------

#Maximum number of applications recieved.
maxappcity= sqlContext.sql("Select job_info_work_city, count(case_status) as application_count  from usvisa where job_info_work_city!='' group by job_info_work_city order by  application_count DESC ")
maxappcity.show()

# COMMAND ----------

#Maximum nuber of acceptances are in the following cities,
maxacceptances=sqlContext.sql("Select Count(case_status) as acceptance_count ,job_info_work_city from usvisa where case_status='Certified' and job_info_work_city!=''  group by job_info_work_city order by acceptance_count DESC")
maxacceptances.show()

# COMMAND ----------

#Maximum number of rejections,
maxrejections=sqlContext.sql("Select Count(case_status) as rejection_count ,job_info_work_city from usvisa where case_status='Denied' and job_info_work_city!=''  group by job_info_work_city order by rejection_count DESC")
maxrejections.show()

# COMMAND ----------

#City Acceptance Ratio
city_acceptance_ratio = maxappcity\
    .join(maxacceptances, "job_info_work_city")\
    .withColumn("ratio", (F.col("acceptance_count") / F.col("application_count")))
    

city_acceptance_ratio.sort('ratio', ascending=False)
city_acceptance_ratio.sort('ratio', ascending=False).filter(F.col("application_count") > 100).show()
#city_acceptance_ratio.sort('ratio', ascending=False).filter(F.col("job_info_work_city") =='New York').show()

# COMMAND ----------

#City Rejection Ratio
city_rejection_ratio = maxappcity\
    .join(maxrejections, "job_info_work_city")\
    .withColumn("ratio", (F.col("rejection_count") / F.col("application_count")))
    
city_rejection_ratio.sort('ratio', ascending=False)
city_rejection_ratio.sort('ratio', ascending=False).filter(F.col("application_count") > 100).show()
city_rejection_ratio.sort('ratio', ascending=False).filter(F.col("job_info_work_city") =='New York').show()

# COMMAND ----------

#Application Rate
sector_application_rate = sqlContext.sql("Select naics_2007_us_title as sector, count(case_status) as app_Count  from usvisa where naics_2007_us_title <> 'null' group by sector order by  app_Count DESC")
sector_application_rate.show(10)

# COMMAND ----------

#Acceptance Rate
sector_acceptance_rate= sqlContext.sql("Select naics_2007_us_title as sector, count(case_status) as acc_count  from usvisa where case_status='Certified' AND  naics_2007_us_title <> 'null' group by sector order by  acc_count DESC")
sector_acceptance_rate.show(10)

# COMMAND ----------

#Acceptance Ratio
sector_acceptance_ratio = sector_application_rate\
    .join(sector_acceptance_rate, "sector")\
    .withColumn("ratio", (F.col("acc_count") / F.col("app_count")))
    
sector_acceptance_ratio.sort('ratio', ascending=False)
sector_acceptance_ratio.sort('ratio', ascending=False).filter(F.col("app_count") > 100).show(10)
