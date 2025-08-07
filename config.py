import os
from dotenv import load_dotenv

load_dotenv()

# Bot Configuration
TOKEN = os.getenv('BOT_TOKEN')
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')


# PostgreSQL Configuration (Neon DB)
NEON_URI = os.getenv('NEON_URI', 'postgresql://neondb_owner:npg_47XBIfzlKvmj@ep-broad-sky-a1rmt03x-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require&pool_timeout=10')
DATABASE_NAME= 'neondb'
CATBOX_USERHASH = os.getenv('CATBOX_USERHASH', '0d6e2b43bfd1b9b505ee6d3df')
IMGUR_CLIENT_ID = os.getenv('IMGUR_CLIENT_ID', '')
BOT_VERSION = os.getenv('BOT_VERSION', '1.0.0')
LOG_CHANNEL_ID = -1002836765689
DROPTIME_LOG_CHANNEL = -1002558794123

OWNER_ID = 6055447708

# Game Configuration
STARTING_COINS = 100
DAILY_REWARD = 50



