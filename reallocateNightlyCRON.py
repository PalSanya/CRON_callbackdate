from common_funs.common import *
import datetime
from common_funs.connection import *

mysql_conn = MySQLConnection()
connection = mysql_conn.get_connection()


def reallocateCallBackTomorrow():
    try:
        # getting records for reallocation
        reallocateCallBackTomorrowCursor = connection.cursor()
        reallocateCallBackTomorrowQuery = """
        SELECT bca.record_uid, bca.date_of_record, bca.update_date, bca.parent_user_uid, bca.user_uid, bca.call_record_uid, bca.alloc_sequence
        FROM callingApp.b2b_calling_alloc bca
        WHERE DATE(bca.update_date) = CURDATE() - INTERVAL 1 DAY 
        AND bca.lead_status = 2 
        AND bca.connect_status = 2
        AND bca.alloc_sequence <= 10;
        """
        reallocateCallBackTomorrowCursor.execute(reallocateCallBackTomorrowQuery)
        reallocateCallBackTomorrowRecords = reallocateCallBackTomorrowCursor.fetchall()
        reallocateCallBackTomorrowCursor.close()
        #print("Retrieved records:", reallocateCallBackTomorrowRecords)
        
        if len(reallocateCallBackTomorrowRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateCallBackTomorrowRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateCallBackTomorrowRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time} Call back tomorrow record reallocated successfully")

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}
    
def reallocateCallBackdate():
    try:
        # getting records for reallocation
        reallocateCallBackTomorrowCursor = connection.cursor()
        reallocateCallBackTomorrowQuery = """
        SELECT bca.record_uid, bca.date_of_record, bca.update_date, bca.parent_user_uid, bca.user_uid, bca.call_record_uid, bca.alloc_sequence
        FROM callingApp.b2b_calling_alloc bca
        WHERE DATE(bca.call_back_date) = CURDATE() - INTERVAL 1 DAY 
        AND bca.lead_status = 2 
        AND bca.connect_status = 2
        AND bca.alloc_sequence <= 10;
        """
        reallocateCallBackTomorrowCursor.execute(reallocateCallBackTomorrowQuery)
        reallocateCallBackTomorrowRecords = reallocateCallBackTomorrowCursor.fetchall()
        reallocateCallBackTomorrowCursor.close()
        #print("Retrieved records:", reallocateCallBackTomorrowRecords)
        
        if len(reallocateCallBackTomorrowRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateCallBackTomorrowRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateCallBackTomorrowRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time} Call back date record reallocated successfully")

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}

def reallocateCallBackDayAfter():
    try:
        # getting records for reallocation
        reallocateCallBackDayAfterCursor = connection.cursor()
        reallocateCallBackDayAfterQuery = """
        SELECT record_uid, date_of_record, update_date, parent_user_uid, user_uid, call_record_uid, alloc_sequence
        FROM callingApp.b2b_calling_alloc
        WHERE DATE(update_date) <= CURDATE() - INTERVAL 2 DAY
        AND lead_status = 3
        AND connect_status = 2
        AND bca.alloc_sequence <= 10
        """
        reallocateCallBackDayAfterCursor.execute(reallocateCallBackDayAfterQuery)
        reallocateCallBackDayAfterRecords = reallocateCallBackDayAfterCursor.fetchall()
        reallocateCallBackDayAfterCursor.close()
        #print("Retrieved records:", reallocateCallBackDayAfterRecords)
        
        if len(reallocateCallBackDayAfterRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateCallBackDayAfterRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateCallBackDayAfterRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time} Call back day after record reallocated successfully")

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}


def reallocateCallBackNextWeek():
    try:
        # getting records for reallocation
        reallocateCallBackNextWeekCursor = connection.cursor()
        reallocateCallBackNextWeekQuery = """
        SELECT record_uid, date_of_record, update_date, parent_user_uid, user_uid, call_record_uid, alloc_sequence
        FROM callingApp.b2b_calling_alloc
        WHERE DATE(update_date) <= CURDATE() - INTERVAL 7 DAY
        AND lead_status = 4
        AND connect_status = 2
        AND bca.alloc_sequence <= 10
        """
        reallocateCallBackNextWeekCursor.execute(reallocateCallBackNextWeekQuery)
        reallocateCallBackNextWeekRecords = reallocateCallBackNextWeekCursor.fetchall()
        reallocateCallBackNextWeekCursor.close()
        #print("Retrieved records:", reallocateCallBackNextWeekRecords)
        
        if len(reallocateCallBackNextWeekRecords)>0:
            # copying data to b2b_calling_alloc_history
            copyRecordToHistory(reallocateCallBackNextWeekRecords)

            # Increment alloc_sequence by 1 in b2b_calling_alloc
            incrementAllocSeq(reallocateCallBackNextWeekRecords)

            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time} Call back next week record reallocated successfully")

    except Exception as e:
        print("Error:", e)
        return {"status": "failure", "data": f"Error: {e}"}

current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"{current_time} process started successfully")
reallocateCallBackTomorrow()
reallocateCallBackDayAfter()
reallocateCallBackNextWeek()
end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"{end_time} process ended")

