USA Permanent visa analysis 

Abstract

A permanent labor certification issued by the Department of Labor (DOL) allows an employer to hire a foreign worker to work permanently in the United States. In most instances, before the U.S. employer can submit an immigration petition to the Department of Homeland Security’s U.S. Citizenship and Immigration Services (USCIS), the employer must obtain a certified labor certification application from the DOL’s Employment and Training Administration (ETA). The DOL must certify to the USCIS that there are not sufficient U.S. workers able, willing, qualified and available to accept the job opportunity in the area of intended employment and that employment of the foreign worker will not adversely affect the wages and working conditions of similarly employed U.S. workers.
Data covers 2012-2017 and includes information on employer, position, wage offered, job posting history, employee education and past visa history, associated lawyers, and final decision.



About the dataset

Data covers 2012-2017 and includes information on employer, position, wage offered, job posting history, employee education and past visa history, associated lawyers, and final decision.

The CSV file contains data of 42,194 applicants across . The data is rich and spread across the occupation of each applicant, their position, education, final decision and more. Along with that the data is historically rich with the job hosting history and applicant’s past visa history. This 330 MB data is really tough to be accessed on Excel let alone perform computational and hence we would be using Spark SQL for the computation 

The few primary column of Data relevant for this EDA is mentioned below. 
1. Wage Offered
2. Application Case status
3. Country of Citizenship 
4. Applicant Name
5. Job training information


Project Introduction

The primary reason of picking this project was to observe any trend with VISA application process of USA Government’s Department of Labor. This data was rich with indexes over the applicant information. It was a good dataset as we could inference a lot from the profile of applicant. Although we faced few challenges when we started to analyse this data. The primary being the sheer size of data. It is 330 MB size of data with 42,194 Rows and 154 Columns. It became a tough task for Excel to handle, and as per our intention of mining the data using SQL operation would be a little too much for Excel. Hence we decided to Using Spark SQL for this operation. Spark is a Big data Technology, built on the principles of Hadoop. 

Having said that, Spark also had some limitations we had to overcome. As you would know, in a traditional set-up of Big Data project. We store the data in an HDFS system in form of RDDs and then use spark technologies we compute and analyse this data, however, 330 MB was using a lot of local computational memory. Hence we decided with going with Cloud based computation. We picked Databricks, a big data tool based on Python and built by Apache. 

It offers us a DBFS, which is data bricks file system, which is a cloud based file storage system. We can use the file stored on their server to compute thus reducing the location computation power requirement. This helped us perform faster computation and analysis of the file. 

We would also use Tableau for the visualisation. Spark is really good with Computation but is not the best tool when it comes to the Visualisation. Here it begins. 
