CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDate` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfdate` bigint,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://my-very-own-stedi-project-bucket/customer_landing/'
TBLPROPERTIES ('classification' = 'json');
