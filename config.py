import os
from dotenv import load_dotenv

load_dotenv()

# Bot Configuration
TOKEN = os.getenv('BOT_TOKEN')
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')


# PostgreSQL Configuration (Neon DB)
NEON_URI = os.getenv('NEON_URI', 'postgresql://neondb_owner:npg_vyeSFHK7r3Eq@ep-snowy-sky-a1hx5dig-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')

CATBOX_USERHASH = os.getenv('CATBOX_USERHASH', '0d6e2b43bfd1b9b505ee6d3df')
IMGUR_CLIENT_ID = os.getenv('IMGUR_CLIENT_ID', '')
BOT_VERSION = os.getenv('BOT_VERSION', '1.0.0')
LOG_CHANNEL_ID = -1002836765689
DROPTIME_LOG_CHANNEL = -1002558794123

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/marvelx')
DATABASE_NAME = "marvelx"

OWNER_ID = 6055447708

# Game Configuration
STARTING_COINS = 100
DAILY_REWARD = 50
