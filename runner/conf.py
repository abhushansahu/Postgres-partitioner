database_connection_string = {
    'host': '',
    'port': 5432,
    'user': '',
    'passw': '',
    'database': ''
}

table_list = [
    {'table_name': '', # test
    'table_sequence_id': '', # test_col_sequence_id
     'temp_table_name': '', # temp_test
     'new_table_name': '', # new_test
     'backup_table_name': '', # test_old
     'short_hand_name_for_child': '', # tst
     'primary_column': '', # id
     'date_column_name': '', # most_frequent_date, if_null_less_frequent_date, if_null_least_frequent_date
     'new_date_column_name': '', # combined_date
     'all_columns': {}  # column_name: data_type
     }

]

cfg_complete_run = False
cfg_are_you_sure = False
cfg_verified = False
cfg_divisor = 1000000
