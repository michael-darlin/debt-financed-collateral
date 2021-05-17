from timeit import default_timer as timer
start = timer()
fullstart = start
import defiEvents

try:
    explorer = defiEvents.RecordExplorer()

    # Connect to MySQL
    explorer.connect()

    # Set protocol and version, event type, and stage
    explorer.set_protocol('Maker', '2')
    explorer.set_record('frob')
    explorer.set_stage(0)

    # Print information on production environment
    explorer.print_environ()
    
    # Query BigQuery and process results
    explorer.run_bq_query(11700000, 1700000)
    explorer.transform_results()
    explorer.print_results()
except defiEvents.DataValidationError as error:
    print(f'ERROR: {error.message}')
finally:
    end = timer()
    print("Total time : %.2f s \n" % (end - fullstart))