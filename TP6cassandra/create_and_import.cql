-- إنشاء قاعدة البيانات
CREATE KEYSPACE IF NOT EXISTS resto_NY
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1 };

USE resto_NY;

-- جدول Restaurant
CREATE TABLE IF NOT EXISTS Restaurant (
  id INT,
  name VARCHAR,
  borough VARCHAR,
  buildingnum VARCHAR,
  street VARCHAR,
  zipcode INT,
  phone TEXT,
  cuisinetype VARCHAR,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_rest_cuisine ON Restaurant (cuisinetype);
CREATE INDEX IF NOT EXISTS idx_rest_borough ON Restaurant (borough);

-- جدول Inspection
CREATE TABLE IF NOT EXISTS Inspection (
  idrestaurant INT,
  inspectiondate DATE,
  violationcode VARCHAR,
  violationdescription VARCHAR,
  criticalflag VARCHAR,
  score INT,
  grade VARCHAR,
  PRIMARY KEY (idrestaurant, inspectiondate)
);

CREATE INDEX IF NOT EXISTS idx_ins_grade ON Inspection (grade);
CREATE INDEX IF NOT EXISTS idx_ins_score ON Inspection (score);

-- استيراد الملفات CSV
COPY Restaurant (id, name, borough, buildingnum, street, zipcode, phone, cuisinetype)
FROM '/restaurants.csv' WITH DELIMITER=',' AND HEADER = TRUE;

COPY Inspection (idrestaurant, inspectiondate, violationcode, violationdescription, criticalflag, score, grade)
FROM '/restaurants_inspections.csv' WITH DELIMITER=',' AND HEADER = TRUE;

-- التحقق من عدد البيانات
SELECT count(*) FROM Restaurant;
SELECT count(*) FROM Inspection;

