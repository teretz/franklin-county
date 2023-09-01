
import pandas as pd

def split_address(full_address):
    parts = full_address.split(',')
    street = parts[0].strip() if len(parts) > 0 else ''
    city = parts[1].strip() if len(parts) > 1 else ''
    state_zip = parts[2].strip() if len(parts) > 2 else ''
    state = state_zip.split(' ')[0] if state_zip else ''
    zip_code = state_zip.split(' ')[1] if state_zip and len(state_zip.split(' ')) > 1 else ''
    return street, city, state, zip_code

def handle_name_line(row_data):
    global current_record
    if len(row_data) >= 2:
        name_parts = row_data[0].split(",")
        last_name = name_parts[0].strip() if len(name_parts) > 0 else ''
        first_parts = name_parts[1].strip().split(" ") if len(name_parts) > 1 else []
        first_name = first_parts[0] if len(first_parts) > 0 else ''
        middle_name = first_parts[1] if len(first_parts) > 1 else ''
        address_parts = split_address(row_data[1])
        current_record.update({
            'LastName': last_name,
            'FirstName': first_name,
            'MiddleName': middle_name,
            'Address': address_parts[0],
            'City': address_parts[1],
            'State': address_parts[2],
            'ZipCode': address_parts[3],
            'ArrestStatus': row_data[2] if len(row_data) > 2 else ''
        })

def handle_charge_desc_line(row_data):
    global current_record, charge_counter
    if len(row_data) > 0:
        charge_desc = row_data[0].strip()
        current_record[f'Charge{charge_counter}Desc'] = charge_desc
        charge_counter += 1

final_records_test = []
current_record = {}
charge_counter = 1

def reset_variables():
    global current_record, charge_counter
    current_record = {}
    charge_counter = 1

def flush_current_record():
    global final_records_test
    if current_record:
        final_records_test.append(current_record)
    reset_variables()

input_df = pd.read_csv('input.csv', header=None)

for index, row in input_df.iterrows():
    row_data = row.dropna().tolist()
    if len(row_data) >= 3:
        flush_current_record()
        handle_name_line(row_data)
    elif len(row_data) == 1:
        handle_charge_desc_line(row_data)
        
flush_current_record()

final_output_df_test = pd.DataFrame(final_records_test)
final_output_df_test.to_csv('output.csv', index=False)
