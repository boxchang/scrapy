import time
from fugle_realtime import WebSocketClient

def handle_message(message):
    print(message)

def main():
    ws_client = WebSocketClient(api_token='174047bf6e7c04d1006406339ca65eea')
    ws = ws_client.intraday.quote(symbolId='6121', on_message=handle_message)
    ws.run_async()
    time.sleep(5)
    ws.close()

if __name__ == '__main__':
    main()