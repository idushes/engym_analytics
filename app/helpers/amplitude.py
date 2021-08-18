import requests
import os

headers = {
  'Content-Type': 'application/json',
  'Accept': '*/*'
}

def send_events(events):
  json = {
    "api_key": os.environ.get("AMPLITUDE_API_KEY"),
    "events": events
  }
  r = requests.post('https://api2.amplitude.com/2/httpapi', params={}, headers=headers, json=json)
  return r.json()