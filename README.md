# Data Partitioner
Project to handle table partitioning task so that data can be processed faster in postgres environment.
It can also be used as Data Archival if followed the PostRun step below.

### Pre-requisite
1. Python environment(preferably 3.7)
1. Virtual env with packages installed from [requirements.txt](runner/requirements.txt)
1. Host machine with access to postgres instance endpoint
1. The instance should be checked for the storage and must be set to dynamically increase the storage, at most twice the current size depending on the run cycle

Preferably when the postgres instance is running as an RDS instance in AWS

### Process
What happens whe the [code](runner/run.py) is ran. 

It creates a new partitioned table of the previously giant un-partitioned one a monthly schema.<br> 
This process reduces the stress in the production cost for inserts, select and deletes.<br>
This process reduces table bloating, although it might increase the overall index size, but with an advantage as the index would not be dealing with unbalance Btree.<br>
Once the table is partitioned, the instance will then needs to have a manual snapshot.<br>
This snapshot will be the historical rds which can be restored to get the data for point in time.<br>
Furthermore the current rds being partitioned can get rid of historical data by issuing drop commands to the child table, without impacting any process and resource



### Post Run
Once the process is completed and all tests are done. We need to take the manual snapshot of rds, serving as a historical point in time recovery and drop past child in current master.

#### Test Performed
 A test was performed on the rds "test table" as reference table of 50 cols of size 225 gigs, (needed extra 250gb for new data inserts.)<br/>
 Process took about 10-14 hours to get completed.<br/>
 Switch masters was run separately will the [script](test/bulk_insert.py) running and creating a process of rapid data insertion.<br/>
 Master switch took at max 1 min to get completed.
 
#### Concerns
After the switch was performed, there is some data diff between the old master and new master. <br/>
It can be solved by changing the sequence id of new master to max+1 of old master. And then inserting the diff records seaprately with defined primary column in the insert statements. 
 
