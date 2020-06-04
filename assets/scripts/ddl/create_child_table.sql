CREATE TABLE if not exists {1}_inconsistent_data (CONSTRAINT pk_{1}_inconsistent PRIMARY KEY ({2})) INHERITS ({0});
CREATE TABLE if not exists {1}_2018 (CONSTRAINT pk_{1}_2018 PRIMARY KEY ({2})) INHERITS ({0});
CREATE TABLE if not exists {1}_2017 (CONSTRAINT pk_{1}_2017 PRIMARY KEY ({2})) INHERITS ({0});
CREATE TABLE if not exists {1}_2017_before (CONSTRAINT pk_2017_before PRIMARY KEY ({2})) INHERITS ({0});
CREATE INDEX if not exists idx_{1}_2018 ON {1}_2018 ({3});
CREATE INDEX if not exists idx_{1}_2017 ON {1}_2017 ({3});
CREATE INDEX if not exists idx_{1}_2017_before ON {1}_2017_before ({3});
|
CREATE TABLE if not exists {1}_{4} (CONSTRAINT pk_{1}_{4} PRIMARY KEY ({2})) INHERITS ({0});
CREATE INDEX if not exists idx_{1}_{4} ON {1}_{4} ({3});