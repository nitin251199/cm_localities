import requests
import pandas as pd


def process_record(record: str):
    text_record = {}
    text_array = record.split(',')
    for i in range(len(text_array)):
        text_record[f"data{i + 1}"] = text_array[i]
    return text_record


def fetch_data(url, cursor=None):
    params = {"fields": "id,txt"} if cursor else {}
    if cursor is not None:
        url += "&cursor=%s" % cursor
    response = requests.get(url, params=params, headers={
        "Authorization": "Token {token}".format(
            token="006cc23edb0c2a0af420dfa107cfbb6a"
        )
    })
    response.raise_for_status()
    return response.json()


def append_to_csv(data, csv_file):
    df = pd.DataFrame(data, columns=['data1', 'data2', 'data3', 'data4', 'data5'])
    df.to_csv(csv_file, mode='a', header=not pd.read_csv(csv_file).empty, index=False)


def main():
    initial_url = "https://api.mindat.org/localities/?format=json"
    csv_file = 'data.csv'
    cursor = 'cD04NTIxNQ%3D%3D'
    # last_updated_id = None

    while True:
        try:
            response_data = fetch_data(initial_url, cursor)
            results = response_data.get('results', [])
            next_url = response_data.get('next')

            # Extract 'txt' key and 'id'
            data_to_append = [process_record(record["txt"]) for record in results if 'txt' in record]

            # Append data to CSV
            append_to_csv(data_to_append, csv_file)

            # # Update the last_updated_id
            # if data_to_append:
            #     last_updated_id = data_to_append[-1]['id']

            # If there is no next URL, break the loop
            if not next_url:
                break

            # Update the cursor for the next iteration
            cursor = next_url.split('cursor=')[1].split('&')[0] if 'cursor=' in next_url else None
            print("Fetching cursor: ", cursor)

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            continue
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    print(f"Last updated Cursor: {cursor}")


if __name__ == '__main__':
    main()
