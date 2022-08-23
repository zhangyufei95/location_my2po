create table stage.stage_users_test0822 (
                                        id string comment 'id',
                                        user_name string comment 'user_name',
                                        PRIMARY KEY (id)
)
    PARTITION BY HASH (id) PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='172.27.16.41:7051,172.27.16.43:7051,172.27.16.49:7051')