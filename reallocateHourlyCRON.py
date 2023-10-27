from common_funs.common import *
import datetime
from common_funs.connection import *

mysql_conn = MySQLConnection()
connection = mysql_conn.get_connection()


def reallocateNotConnected():
    try:
        # getting records for reallocation
        reallocateNotConnectedCursor = connection.cursor()
        reallocateNotConnectedQuery = """
        SELECT bca.record_uid, bca.date_of_record, bca.update_date, bca.parent_user_uid, bca.user_uid, bca.call_record_uid, bca.alloc_sequence
        FROM callingApp.b2b_calling_alloc bca
        LEFT JOIN callingApp.calling_transaction ct 
        ON bca.record_uid = ct.record_uid
        WHERE DATE(bca.update_date) >= CURDATE() - INTERVAL 2 HOUR 
        AND ct.connect_status = 1
        AND bca.alloc_sequence <= 10;
        """
        reallocateNotConnectedCursor.execute(reallocateNotConnectedQuery)
        reallocateNotConnectedRecords = reallocateNotConnectedCursor.fetchall()
        reallocateNotConnectedCursor.close()
        print("Retrieved records:", reallocateNotConnectedRecords)

        if len(reallocateNotConnectedRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateNotConnectedRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateNotConnectedRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time} Not connected record reallocated successfully")

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}



def reallocateSkipped():
    try:
        # getting records for reallocation
        reallocateSkippedCursor = connection.cursor()
        reallocateSkippedQuery = """
        SELECT bca.record_uid, bca.date_of_record, bca.update_date, bca.parent_user_uid, bca.user_uid, bca.call_record_uid, bca.alloc_sequence
        FROM callingApp.b2b_calling_alloc bca
        WHERE DATE(bca.update_date) >= CURDATE() - INTERVAL 4 HOUR
        AND bca.call_record_uid = '-1'
        AND bca.alloc_sequence <= 10
        """
        reallocateSkippedCursor.execute(reallocateSkippedQuery)
        reallocateSkippedRecords = reallocateSkippedCursor.fetchall()
        reallocateSkippedCursor.close()
        #print("Retrieved records:", reallocateSkippedRecords)
        
        if len(reallocateSkippedRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateSkippedRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateSkippedRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            print(f'{current_time}: Skipped record reallocated successfully')

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}




current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"{current_time} process started successfully")
reallocateSkipped()
reallocateNotConnected()
end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"{end_time} process ended")