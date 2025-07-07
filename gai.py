import os
import sqlite3
import json
import csv
import asyncio
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, User, PeerChannel
from telethon.errors import FloodWaitError, RPCError
import aiohttp
import sys
import logging
import os
from datetime import datetime
from tqdm import tqdm

# 定义日志文件夹路径
log_folder = 'logs'
# 如果日志文件夹不存在，则创建它
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
script_name = os.path.splitext(os.path.basename(__file__))[0]
current_time = datetime.now().strftime("%Y%m%d%H%M%S")
log_file_name = f"{script_name}_{current_time}.log"
log_file_path = os.path.join(log_folder, log_file_name)
# 基本配置，将日志输出到指定文件夹下的文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=log_file_path
)
# 记录日志
logging.info('程序开始')


def display_ascii_art():
    WHITE = "\033[97m"
    RESET = "\033[0m"
    
    art = r"""
___________________  _________
\__    ___/  _____/ /   _____/
  |    | /   \  ___ \_____  \ 
  |    | \    \_\  \/        \
  |____|  \______  /_______  /
                 \/        \/
    """
    
    print(WHITE + art + RESET)

display_ascii_art()

STATE_FILE = 'state.json'

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            logging.info('读取状态文件')
            return json.load(f)
    return {
        'api_id': None,
        'api_hash': None,
        'phone': None,
        'channels': {},
        'scrape_media': True,
    }

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        # logging.info('保存状态文件')
        json.dump(state, f)

state = load_state()

logging.info('状态文件加载完成')

if not state['api_id'] or not state['api_hash'] or not state['phone']:
    state['api_id'] = int(input("Enter your API ID: "))
    state['api_hash'] = input("Enter your API Hash: ")
    state['phone'] = input("Enter your phone number: ")
    save_state(state)

client = TelegramClient('session', state['api_id'], state['api_hash'])
logging.info('客户端连接完成')
def save_message_to_db(channel, message, sender):
    channel_dir = os.path.join(os.getcwd(), channel)
    os.makedirs(channel_dir, exist_ok=True)

    db_file = os.path.join(channel_dir, f'{channel}.db')
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS messages
                  (id INTEGER PRIMARY KEY, 
                  message_id INTEGER, 
                  date TEXT, 
                  sender_id INTEGER, 
                  first_name TEXT, 
                  last_name TEXT, 
                  username TEXT, 
                  message TEXT, 
                  media_type TEXT, 
                  media_path TEXT, 
                  reply_to INTEGER)''')
    c.execute('''INSERT OR IGNORE INTO messages (message_id, date, sender_id, first_name, last_name, username, message, media_type, media_path, reply_to)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (message.id, 
               message.date.strftime('%Y-%m-%d %H:%M:%S'), 
               message.sender_id,
               getattr(sender, 'first_name', None) if isinstance(sender, User) else None, 
               getattr(sender, 'last_name', None) if isinstance(sender, User) else None,
               getattr(sender, 'username', None) if isinstance(sender, User) else None,
               message.message, 
               message.media.__class__.__name__ if message.media else None, 
               None,
               message.reply_to_msg_id if message.reply_to else None))
    conn.commit()
    conn.close()

MAX_RETRIES = 5

async def download_media(channel, message):
    if not message.media or not state['scrape_media']:
        return None

    channel_dir = os.path.join('/root/webdav/TG', channel)
    media_folder = os.path.join(channel_dir, 'media')
    os.makedirs(media_folder, exist_ok=True)
    media_file_name = None
    if isinstance(message.media, MessageMediaPhoto):
        media_file_name = message.file.name or f"{message.id}.jpg"
        return None
    elif isinstance(message.media, MessageMediaDocument):
        media_file_name = message.file.name or f"{message.id}.{message.file.ext if message.file.ext else 'bin'}"

    if not media_file_name:
        return None

    media_path = os.path.join(media_folder, media_file_name)

    if os.path.exists(media_path):
        return media_path

    retries = 0
    while retries < MAX_RETRIES:
        try:
            if isinstance(message.media, MessageMediaPhoto):
                media_path = await message.download_media(file=media_folder)
            elif isinstance(message.media, MessageMediaDocument):
                media_path = await message.download_media(file=media_folder)
            if media_path:
                print(f"Successfully downloaded media to: {media_path}")
            break
        except (TimeoutError, aiohttp.ClientError, RPCError) as e:
            retries += 1
            print(f"Retrying download for message {message.id}. Attempt {retries}...")
            await asyncio.sleep(2 ** retries)
    return media_path

    retries = 0
    while retries < MAX_RETRIES:
        try:
            if isinstance(message.media, MessageMediaPhoto):
                logging.info("message.media为MessageMediaPhoto")
                media_path = await message.download_media(file=media_folder)
            elif isinstance(message.media, MessageMediaDocument):
                logging.info("message.media为MessageMediaDocument")
                media_path = await message.download_media(file=media_folder)
            if media_path:

                logging.info(f"Successfully downloaded media to: {media_path}")
            break
        except (TimeoutError, aiohttp.ClientError, RPCError) as e:
            retries += 1
            print(f"Retrying download for message {message.id}. Attempt {retries}...")
            logging.info(f"Retrying download for message {message.id}. Attempt {retries}...")
            logging.info(f"sleep {2 ** retries}s")
            await asyncio.sleep(2 ** retries)
    return media_path

async def rescrape_media(channel):
    channel_dir = os.path.join(os.getcwd(), channel)
    db_file = os.path.join(channel_dir, f'{channel}.db')
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT message_id FROM messages WHERE media_type IS NOT NULL AND media_path IS NULL')
    rows = c.fetchall()
    conn.close()

    total_messages = len(rows)
    if total_messages == 0:
        print(f"No media files to reprocess for channel {channel}.")
        return

    for index, (message_id,) in enumerate(rows):
        try:
            entity = await client.get_entity(PeerChannel(int(channel)))
            message = await client.get_messages(entity, ids=message_id)
            media_path = await download_media(channel, message)
            if media_path:
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                c.execute('''UPDATE messages SET media_path = ? WHERE message_id = ?''', (media_path, message_id))
                conn.commit()
                conn.close()
            
            progress = (index + 1) / total_messages * 100
            sys.stdout.write(f"\rReprocessing media for channel {channel}: {progress:.2f}% complete")
            sys.stdout.flush()
        except Exception as e:
            print(f"Error reprocessing message {message_id}: {e}")
    print()

async def scrape_channel(channel, offset_id):
    try:
        if channel.startswith('-'):
            entity = await client.get_entity(PeerChannel(int(channel)))
        else:
            entity = await client.get_entity(channel)
        logging.info("get entity end!")
        total_messages = 0
        processed_messages = 0

        logging.info("统计总信息数 ")
        result = await client.get_messages(entity, offset_id=offset_id, reverse=True, limit=0)
        total_messages = result.total

        if total_messages == 0:
            print(f"No messages found in channel {channel}.")
            return
        logging.info("开始爬取")
        last_message_id = None
        processed_messages = 0
        initial_value = state['channels'][channel]
        with tqdm(total=total_messages, desc=f"Scraping channel: {channel}", unit="msg",initial=initial_value) as pbar:
            async for message in client.iter_messages(entity, offset_id=offset_id, reverse=True):
                try:
                    sender = await message.get_sender()
                    save_message_to_db(channel, message, sender)

                    if state['scrape_media'] and message.media:
                        media_path = await download_media(channel, message)
                        if media_path:
                            conn = sqlite3.connect(os.path.join(channel, f'{channel}.db'))
                            c = conn.cursor()
                            c.execute('''UPDATE messages SET media_path = ? WHERE message_id = ?''', (media_path, message.id))
                            conn.commit()
                            conn.close()
                    
                    last_message_id = message.id
                    processed_messages += 1

                    pbar.update(1)

                    state['channels'][channel] = last_message_id
                    save_state(state)
                except Exception as e:
                    print(f"Error processing message {message.id}: {e}")
            print()
    except ValueError as e:
        print(f"Error with channel {channel}: {e}")

async def continuous_scraping():
    global continuous_scraping_active
    continuous_scraping_active = True

    try:
        while continuous_scraping_active:
            for channel in state['channels']:
                print(f"\nChecking for new messages in channel: {channel}")
                await scrape_channel(channel, state['channels'][channel])
                print(f"New messages or media scraped from channel: {channel}")
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Continuous scraping stopped.")
        continuous_scraping_active = False

async def export_data():
    for channel in state['channels']:
        if state['channels'][channel] == 0:
            print(f"No messages to export for channel {channel}. Skipping export.")
            continue
        export_to_csv(channel)
        export_to_json(channel)

def export_to_csv(channel):
    db_file = os.path.join(channel, f'{channel}.db')
    csv_file = os.path.join(channel, f'{channel}.csv')
    # print(f"Trying to open database file: {db_file}")
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT * FROM messages')
    rows = c.fetchall()
    conn.close()

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([description[0] for description in c.description])
        writer.writerows(rows)

def export_to_json(channel):
    db_file = os.path.join(channel, f'{channel}.db')
    json_file = os.path.join(channel, f'{channel}.json')
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('SELECT * FROM messages')
    rows = c.fetchall()
    conn.close()

    data = [dict(zip([description[0] for description in c.description], row)) for row in rows]
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

async def view_channels():
    if not state['channels']:
        print("No channels to view.")
        return
    
    print("\nCurrent channels:")
    for channel, last_id in state['channels'].items():
        print(f"Channel ID: {channel}, Last Message ID: {last_id}")

async def list_Channels():
    try:
        print("\nList of channels joined by account: ")
        async for dialog in client.iter_dialogs():
            if (dialog.id != 777000):
                print(f"* {dialog.title} (id: {dialog.id})")
    except Exception as e:
        print(f"Error processing: {e}")


async def manage_channels():
    while True:
        print("\nMenu:")
        print("[A] Add new channel")
        print("[addOnList] Add new channel from csv file")
        print("[R] Remove channel")
        print("[S] Scrape all channels")
        print("[M] Toggle media scraping (currently {})".format(
            "enabled" if state['scrape_media'] else "disabled"))
        print("[C] Continuous scraping")
        print("[E] Export data")
        print("[V] View saved channels")
        print("[L] List account channels")
        
        print("[Q] Quit")

        choice = input("Enter your choice: ").lower()
        match (choice):
            case 'a':
                channel = input("Enter channel ID: ")
                state['channels'][channel] = 0
                save_state(state)
                print(f"Added channel {channel}.")
            case 'r':
                channel = input("Enter channel ID to remove: ")
                if channel in state['channels']:
                    del state['channels'][channel]
                    save_state(state)
                    print(f"Removed channel {channel}.")
                else:
                    print(f"Channel {channel} not found.")
            case 's':
                for channel in state['channels']:
                    await scrape_channel(channel, state['channels'][channel])
            case 'm':
                state['scrape_media'] = not state['scrape_media']
                save_state(state)
                print(
                    f"Media scraping {'enabled' if state['scrape_media'] else 'disabled'}.")
            case 'c':
                global continuous_scraping_active
                continuous_scraping_active = True
                task = asyncio.create_task(continuous_scraping())
                print("Continuous scraping started. Press Ctrl+C to stop.")
                try:
                    await asyncio.sleep(float('inf'))
                except KeyboardInterrupt:
                    continuous_scraping_active = False
                    task.cancel()
                    print("\nStopping continuous scraping...")
                    await task
            case 'e':
                await export_data()
            case 'v':
                await view_channels()
            case 'q':
                print("Quitting...")
                sys.exit()
            case 'l':
                await list_Channels()
            case 'addonlist':
                #读取csv的第二列
                channel_list = set(state['channels'].keys())
                print("Reading csv file...")
                with open('username.csv', 'r') as f:
                    reader = csv.reader(f)
                    for row in tqdm(reader, desc="Processing channels"):
                        row[1] = row[1].strip().lstrip('@')
                        if row[1] not in channel_list:
                            state['channels'][row[1]] = 0
                save_state(state)
                    
                    
            case _:
                print("Invalid option.")

async def main():
    await client.start()
    while True:
        await manage_channels()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting...")
        sys.exit()
