#!/bin/bash
echo CONFIG: $CONFIG;

umask 000;
postgresTPCHClient/src/main/main --config=$CONFIG --data_folder=$DATA_FOLDER --query_clients=$QUERY_CLIENTS --test_name=$TEST_NAME --upd_rate=$UPD_RATE --reset=$RESET --id=$ID --n_reads_txn=$N_READS_TXN --batch_mode=$BATCH_MODE --latency_mode=$LATENCY_MODE --user=$USER --scale=$SF --update_specific_index=$UPDATE_SPECIFIC_INDEX --is_redirect=$IS_REDIRECT --queryNumbers=$QUERIES --queryDuration=$QUERY_DURATION --queryWait=$QUERY_WAIT --uses_views=$USES_VIEWS --stats_frequency=$STATS_FREQUENCY --n_upd_clients=$N_UPD_CLIENTS --do_viewload=$DOES_VIEWLOAD --do_indexload=$DOES_INDEXLOAD --ip=$IP;