import requests

f = open('API_token', 'r')
API_Token = f.read()
f.close()
base_url = 'https://api.recruitee.com/c/95238'


def get_data(endpoint, params={}, pagination=0, json_data_reference=''):
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + API_Token
    }

    if pagination:

        limit, page = pagination, 1
        lst = []

        while True:
            params['page'] = page
            params['limit'] = limit

            response = requests.get(base_url + endpoint, headers=headers, params=params)

            if response.status_code != 200:
                print("Error: ", response.status_code)
                break

            data = response.json()

            if not data[json_data_reference]:
                break

            lst.append(data[json_data_reference])

            # Increment the offset by the limit
            page += 1

        return lst

    else:
        response = requests.get(base_url + endpoint, headers=headers, params=params)

        return response.json()[json_data_reference]
