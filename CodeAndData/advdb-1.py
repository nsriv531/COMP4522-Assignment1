# Adv DB Winter 2024 - 1
from collections import OrderedDict
import csv 
import random
from datetime import datetime

data_base = {}  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = OrderedDict()# <-- You WILL populate this as you go

def recovery_script(log: list, slay, DB_Log: OrderedDict):
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    # Extract unique IDs from transactions
    for transaction in transactions:
        unique_id = transaction[0]
        if unique_id in data_base:
            DB_Log[unique_id] = data_base[unique_id]

    # Flagging the status based on the slay parameter
    if slay == 1:
        for key in DB_Log:
            DB_Log[key]['STATUS'] = 'rolled back'
    elif slay == 2:
        first_key = next(iter(DB_Log))
        DB_Log[first_key]['STATUS'] = 'committed'
        for key in list(DB_Log.keys())[1:]:
            DB_Log[key]['STATUS'] = 'rolled back'
    elif slay == 3:
        committed_keys = ['1', '5']
        for key in committed_keys:
            DB_Log[key]['STATUS'] = 'committed'
        for key in DB_Log.keys() - set(committed_keys):
            DB_Log[key]['STATUS'] = 'rolled back'
            
    for key, value in DB_Log.items():
        if value['STATUS'] == 'committed':
            transaction = [key] + list(value.values())[1:]  # Extracting transaction details
            for item in transactions:
                if item[0] == key:  # Matching transaction ID
                    attribute_to_change = item[1]
                    new_value = item[2]
                    data_base[key][attribute_to_change] = new_value  # Update the specific attribute in data_base
                    break

    # Write changes to transactionsUnsuccesful.csv
    with open('./CodeAndData/transactionsUnsuccesful.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header_written = False
        for entry_id, entry_data in data_base.items():
            if not header_written:
                writer.writerow(['ID'] + list(entry_data.keys())) 
                header_written = True
            writer.writerow([entry_id] + [entry_data.get(attribute, '') for attribute in list(DB_Log.values())[0].keys()])

    print("DB_Log dictionary after recovery:")
    for key, value in DB_Log.items():
        # print(f"Key: {key}, Value: {value}")
        # print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Key: {key}, Value: {value}, Timestamp: {current_timestamp}")
    pass

def transaction_processing(): #<-- Your CODE
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    
    pass

def successful_Transactions(data: dict, new_file_name: str):
    '''
    Write the contents of the database dictionary to a CSV file with a new filename.
    Will ONLY trigger if no failure occurs.
    '''

    for transaction in transactions:
        key = transaction[0]
        attribute = transaction[1]
        new_value = transaction[2]
        if key in data:
            data[key][attribute] = new_value
    with open(new_file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header_written = False
        for entry_id, entry_data in data.items():
            if not header_written:
                writer.writerow(['ID'] + list(entry_data.keys())) 
                header_written = True
            writer.writerow([entry_id] + list(entry_data.values())) 
    csvfile.close() 
            
def read_file(file_name:str) -> dict:
    '''
    Read the contents of a CSV file line-by-line and return a dictionary.
    Assumes the first element of each sublist is the unique identifier (ID).
    '''
    data_base = {}
    with open(file_name, 'r') as reader:
        header = reader.readline().strip().split(',')  # Assuming the first line is the header
        for line in reader:
            line = line.strip().split(',')
            entry_id = line[0]  # Assuming the ID is the first element
            data_base[entry_id] = dict(zip(header[1:], line[1:]))  # Creating a dictionary for each entry
    size = len(data_base)
    print('The data entries BEFORE updates are presented below:')
    for item in data_base.values():
        print(item)
    print(f"\nThere are {size} records in the database, including the header.\n")
    return data_base

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def main():
    global data_base  # Declare data_base as a global variable
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('./CodeAndData/Employees_DB_ADV.csv')  # Update the global data_base variable
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")    #<--- Your CODE (Call function transaction_processing)
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been committed! Changes are permanent.')
                print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
    if must_recover:
        # Call your recovery script
        recovery_script([], failing_transaction_index, DB_Log)
        print(f"Recovery completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # All transactions ended up well
        print("All transactions ended up well.")
        print("Updates to the database were committed!\n")
        successful_Transactions(data_base, './CodeAndData/transactionsSuccesful.csv') #Writes the new csv file if there are no failures.
    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for key, value in data_base.items():
        print(f"Key: {key}, Value: {value}")
        
main()


