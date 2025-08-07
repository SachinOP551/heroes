import random
import asyncio
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
from functools import lru_cache
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from .logging_utils import send_drop_log
from .decorators import admin_only, is_owner, is_sudo, is_og, check_banned
from modules.postgres_database import get_database, get_rarity_emoji, RARITIES, RARITY_EMOJIS, get_rarity_display
from .tdgoal import track_collect_drop
import time
import string
from pyrogram.enums import ChatType
DROPTIME_LOG_CHANNEL = -1002836765689
LOG_CHANNEL_ID = -1002836765689

RARITIES = {
    "Common": 1,
    "Medium": 2,
    "Rare": 3,
    "Legendary": 4,
    "Exclusive": 5,
    "Elite": 6,
    "Limited Edition": 7,
    "Ultimate": 8,
    "Supreme": 9,
    "Mythic": 10,
    "Zenith": 11,
    "Ethereal": 12,
    "Premium": 13
}

active_drops = {}
# Clean deque-based spam tracking with size limits
user_msgs = defaultdict(lambda: deque(maxlen=10))  # Reduced from 10 to prevent memory leaks
SPAM_LIMIT = 6    # Reduced from 6 to be more strict
SPAM_WINDOW = 2      # Reduced from 2 seconds
message_counts = {}  # In-memory message counter
last_drop_time = {}  # Track last drop time per chat
drop_settings_cache = {}  # Cache for drop settings
last_settings_update = {}  # Track last settings update time
SETTINGS_CACHE_TIME = 30  # Reduced from 60 seconds
drop_locks = {}  # Track active drops per chat
drop_expiry_times = {}  # Track exact expiry time for each drop
# Preloaded next character queue per chat with size limit
preloaded_next_character = defaultdict(lambda: deque(maxlen=5))  # Added size limit
collect_locks = defaultdict(asyncio.Lock)
# Add a lock per chat to prevent race conditions
chat_locks = {}
# Add collection locks to prevent race conditions during character collection
collection_locks = defaultdict(asyncio.Lock)
# Track which characters are being collected to prevent double collection
collecting_characters = defaultdict(set)
# Track which users are collecting which characters to prevent spam
user_collecting = defaultdict(set)

# Epic drop captions
DROP_CAPTIONS = [
    "👁️‍🗨️ Tʜᴇ Wᴀᴛᴄʜᴇʀs ʜᴀᴠᴇ ʀᴇᴠᴇᴀʟᴇᴅ ᴀ sɪʟʜᴏᴜᴇᴛᴛᴇ…\nA ғᴏʀᴄᴇ ᴏꜰ ᴘᴏᴡᴇʀ ᴀᴡᴀɪᴛs ɪᴛs ᴍᴀsᴛᴇʀ.\n✴️ /collect name ᴀɴᴅ sᴇᴀʟ ʏᴏᴜʀ ꜰᴀᴛᴇ.",
    "🕯️ Tʜᴇ sᴄʀᴏʟʟs ʜᴀᴠᴇ sᴘᴏᴋᴇɴ...\nA ᴡᴀʀʀɪᴏʀ ᴀʀɪsᴇs ꜰʀᴏᴍ ᴛʜᴇ ᴇᴛᴇʀɴᴀʟ ɢʀɪᴍᴏɪʀᴇ.\n🗝️ /collect name ᴛᴏ ꜰᴜʟꜰɪʟʟ ᴛʜᴇ ᴘʀᴏᴘʜᴇᴄʏ.",
    "🌌 A sᴛᴀʀ ʜᴀs ꜰᴀʟʟᴇɴ ꜰʀᴏᴍ ᴛʜᴇ ᴄᴏsᴍɪᴄ ʀɪᴠᴇʀs…\nA ᴄʜᴀʀᴀᴄᴛᴇʀ ᴀᴡᴀɪᴛs ʏᴏᴜʀ sᴜᴍᴍᴏɴ.\n🪐 Wɪʟʟ ʏᴏᴜ ᴀɴsᴡᴇʀ ᴛʜᴇ ᴄᴀʟʟ? /collect name",
    "⚔️ Fᴀᴛᴇ ᴄᴀʟʟs, ᴀɴᴅ ᴀ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴀɴsᴡᴇʀs.\nTʜᴇ ʙᴀᴛᴛʟᴇ ʙᴇᴛᴡᴇᴇɴ ʟɪɢʜᴛ ᴀɴᴅ sʜᴀᴅᴏᴡ ᴄᴏɴᴛɪɴᴜᴇs…\n✨ /collect name ᴛᴏ ᴄʟᴀɪᴍ ʏᴏᴜʀ ᴀʟʟɪᴇs.",
    "🔮 Tʜᴇ ᴛɪᴍᴇʟɪɴᴇ ᴛᴇᴀʀs ᴀᴘᴀʀᴛ…\nA sᴏᴜʟ sᴛᴇᴘs ꜰᴏʀᴛʜ ᴛᴏ ᴍᴇᴇᴛ ɪᴛs ᴅᴇsᴛɪɴʏ.\n🕰️ Wɪʟʟ ʏᴏᴜ /collect name ᴛʜᴇ ᴄʜᴀᴍᴘɪᴏɴ?",
    "🔥 Fʀᴏᴍ ᴛʜᴇ ᴀsʜᴇs ᴏꜰ ᴡᴀʀ, ᴀ ғɪɢᴜʀᴇ ʀɪsᴇs...\nTʜᴇ ᴇʟᴇᴍᴇɴᴛs ʙᴏᴡ ᴛᴏ ᴛʜᴇɪʀ ᴡɪʟʟ.\n🜂 Cʟᴀɪᴍ ᴛʜᴇɪʀ ꜰɪʀᴇ: /collect name",
    "🌙 Tʜᴇ sʜᴀᴅᴏᴡs sᴛɪʀ… ᴀ sɪʟᴇɴᴛ ꜰᴏʀᴄᴇ ᴀᴘᴘʀᴏᴀᴄʜᴇs.\nWɪʟʟ ʏᴏᴜ ʙʀɪɴɢ ᴛʜᴇᴍ ᴛᴏ ʟɪɢʜᴛ — ᴏʀ ᴊᴏɪɴ ᴛʜᴇɪʀ ᴅᴀʀᴋɴᴇss?\n⚫ /collect name",
    "⚖️ Tʜᴇ sᴄᴀʟᴇs ʜᴀᴠᴇ ᴛɪᴘᴘᴇᴅ...\nA ꜰᴏʀᴄᴇ ᴏꜰ ᴄʜᴀᴏs — ᴏʀ ᴏʀᴅᴇʀ — sᴛᴀɴᴅs ʙᴇꜰᴏʀᴇ ʏᴏᴜ.\n⚔️ /collect name ᴛᴏ ᴅᴇᴄɪᴅᴇ ᴛʜᴇɪʀ ꜰᴀᴛᴇ.",
    "⛓️ ᴀ ʟᴇɢᴇɴᴅ, ʟᴏsᴛ ɪɴ ᴛɪᴍᴇ, ʜᴀs ʙʀᴏᴋᴇɴ ꜰʀᴇᴇ…\nTʜᴇ ᴄᴀʟʟ ɪs ʏᴏᴜʀs ᴛᴏ ᴀɴsᴡᴇʀ.\n🗝️ /collect name ᴛᴏ ᴄʜᴀɪɴ ᴛʜᴇ ᴍʏᴛʜ ᴛᴏ ʏᴏᴜʀ ᴡɪʟʟ.",
    "🛡️ A ɴᴇᴡ ꜰɪɢᴜʀᴇ sᴛᴀɴᴅs ɪɴ ᴛʜᴇ sʜᴀᴅᴏᴡs…\nJᴜsᴛɪᴄᴇ ᴏʀ ᴠᴇɴɢᴇᴀɴᴄᴇ — ʏᴏᴜ ᴅᴇᴄɪᴅᴇ.\n🗡️ Cʟᴀɪᴍ ᴛʜᴇɪʀ ᴘᴀᴛʜ: /collect name",
    "🕸️ Tɪᴍᴇ ᴀɴᴅ sᴘᴀᴄᴇ ᴀʀᴇ ᴛᴀɴɢʟᴇᴅ…\nA ɴᴇᴡ sᴘɪɴ ᴏꜰ ᴛʜᴇ ᴡᴇʙ ʜᴀs ᴄᴀᴜɢʜᴛ ᴀ ʜᴇʀᴏ.\n🔗 Sᴇɪᴢᴇ ᴛʜᴇ ᴛʜʀᴇᴀᴅ: /collect name",
    "🗽 A ɴᴇᴡ ꜰᴏʀᴄᴇ ᴇᴍᴇʀɢᴇs ɪɴ ᴛʜᴇ ᴄɪᴛʏ...\nFʀɪᴇɴᴅ ᴏʀ ꜰᴏᴇ? Tʜᴇ ᴄʜᴏɪᴄᴇ ɪs ʏᴏᴜʀs.\n⚖️ /collect name ᴛᴏ ʙʀɪɴɢ ᴛʜᴇᴍ ᴜɴᴅᴇʀ ʏᴏᴜʀ ᴄᴏᴍᴍᴀɴᴅ.",
    "👑 Tʜᴇ ᴛʜʀᴏɴᴇ ɪs ᴇᴍᴘᴛʏ ɴᴏ ᴍᴏʀᴇ…\nA ʟᴇᴀᴅᴇʀ ʜᴀs ʀᴇᴛᴜʀɴᴇᴅ ᴛᴏ ᴄʟᴀɪᴍ ᴛʜᴇɪʀ ʀɪɢʜᴛ.\n👁️‍🗨️ /collect name ʙᴇꜰᴏʀᴇ ᴛʜᴇʏ ʀᴇɪɢɴ ᴜɴᴄʜᴀʟʟᴇɴɢᴇᴅ"
]

# --- JACKPOT FEATURE ---
active_jackpots = {}  # chat_id: {code, amount, claimed_by, message_id}
jackpot_counter = {}  # chat_id: current count (int)
jackpot_next_interval = {}  # chat_id: next interval (int)

async def drop_jackpot(client, chat_id):
    code = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    amount = random.randint(1000, 2000)
    msg = (
        f"🎰 ᴊᴀᴄᴋᴘᴏᴛ ᴄᴏᴅᴇ ɪs: <code>{code}</code>\n\n"
        f" ᴛᴏ ᴄʟᴀɪᴍ ᴛʜᴇ ᴊᴀᴄᴋᴘᴏᴛ ᴜsᴇ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ: <code>/jackpot {code}</code>\n"
    )
    # Use direct image link for photo
    image_url = "https://ibb.co/TxnK47Sq"
    sent = await client.send_photo(chat_id, image_url, caption=msg)
    active_jackpots[chat_id] = {
        'code': code,
        'amount': amount,
        'claimed_by': None,
        'claimed_by_name': None,
        'message_id': sent.id if hasattr(sent, 'id') else sent.message_id
    }

# --- Jackpot claim command ---
async def jackpot_command(client: Client, message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    db = get_database()
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ Usage: /jackpot code")
        return
    code = args[1].strip()
    jackpot = active_jackpots.get(chat_id)
    if not jackpot or jackpot['code'] != code:
        await message.reply_text("❌ No active jackpot with this code in this group!")
        return
    if jackpot['claimed_by']:
        name = jackpot.get('claimed_by_name', 'someone else')
        await message.reply_text(f"❌ This jackpot was already claimed by {name}!", disable_web_page_preview=True)
        return
    # Mark as claimed
    jackpot['claimed_by'] = user_id
    jackpot['claimed_by_name'] = message.from_user.first_name
    # Add shards to user (update shards only)
    try:
        shards_amount = jackpot['amount']
        await db.users.update_one(
            {'user_id': user_id},
            {'$inc': {'shards': shards_amount}}
        )
        # Log jackpot claim action
        await db.log_user_transaction(user_id, "jackpot_claim", {
            "amount": shards_amount,
            "chat_id": chat_id,
            "code": code,
            "date": datetime.now().strftime('%Y-%m-%d %H:%M')
        })
    except Exception:
        pass
    await message.reply_text(f"🎉 Congratulations! You claimed the jackpot and won <b>{shards_amount}</b> 🎐 Shards!\n\nClaimed by: <a href=\"tg://user?id={user_id}\">{message.from_user.first_name}</a>", disable_web_page_preview=True)
    # Do NOT edit the original jackpot message anymore

# To trigger a jackpot drop, you can call drop_jackpot(client, chat_id) from anywhere, e.g. after a random message count in handle_message or process_drop.

@lru_cache(maxsize=100)
def get_drop_time(chat_id):
    """Get cached drop time for a chat"""
    return message_counts.get(chat_id, 0)

async def get_cached_drop_settings(db, chat_id):
    """Get cached drop settings"""
    current_time = datetime.now()
    if (chat_id not in drop_settings_cache or 
        chat_id not in last_settings_update or 
        (current_time - last_settings_update[chat_id]).total_seconds() > SETTINGS_CACHE_TIME):
        settings = await db.get_drop_settings()
        drop_settings_cache[chat_id] = settings
        last_settings_update[chat_id] = current_time
    return drop_settings_cache[chat_id]

# Periodic cleanup functions removed to prevent issues

# Helper to check if a user is currently banned (auto-expires ban)
async def is_user_banned(user_id):
    import os

# Import database based on configuration
from modules.postgres_database import get_database, get_rarity_emoji, RARITIES, RARITY_EMOJIS, get_rarity_display

# Add in-memory storage for droptime settings and message counts
chat_drop_settings = {}  # chat_id: {'drop_time': int, 'auto_drop': bool}
chat_message_counts = {}  # chat_id: int (current message count)
chat_last_drop_times = {}  # chat_id: datetime (last drop time)

# Default droptime for new chats
DEFAULT_DROPTIME = 60

# Pyrogram message counting handler
async def handle_message(client: Client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    chat_id = message.chat.id
    current_time = datetime.now()

    # Skip private messages
    if message.chat.type == 'private':
        return

    # Completely ignore messages from banned users
    if await is_user_banned(user_id):
        return

    # Start queue processor if not running
    global queue_processor_running
    if not queue_processor_running:
        asyncio.create_task(process_message_queue())

    # Check if we're under high load (queue size > 100)
    if message_queue.qsize() > 100:
        # Use queue for high-volume processing
        try:
            await message_queue.put((client, message, current_time))
        except asyncio.QueueFull:
            # Queue is full, process directly but skip spam detection
            await handle_single_message(client, message, current_time)
        return

    # Normal processing with spam detection
    # --- JACKPOT COUNTER LOGIC ---
    if chat_id not in jackpot_counter:
        jackpot_counter[chat_id] = 0
    if chat_id not in jackpot_next_interval:
        jackpot_next_interval[chat_id] = random.randint(450, 550)
    jackpot_counter[chat_id] += 1
    if jackpot_counter[chat_id] >= jackpot_next_interval[chat_id]:
        asyncio.create_task(drop_jackpot(client, chat_id))
        jackpot_counter[chat_id] = 0
        jackpot_next_interval[chat_id] = random.randint(450, 550)

    # Initialize in-memory settings for this chat if not exists
    if chat_id not in chat_drop_settings:
        chat_drop_settings[chat_id] = {
            'drop_time': DEFAULT_DROPTIME,
            'auto_drop': True
        }
    if chat_id not in chat_message_counts:
        chat_message_counts[chat_id] = 0
    if chat_id not in chat_last_drop_times:
        chat_last_drop_times[chat_id] = current_time
    
    # Get droptime from in-memory settings
    drop_time = chat_drop_settings[chat_id]['drop_time']
    # Increment message count immediately
    chat_message_counts[chat_id] += 1
    # ...existing code...
    # Check if it's time to drop
    if chat_message_counts[chat_id] >= drop_time:
        # ...existing code...
        # Reset message count immediately
        chat_message_counts[chat_id] = 0
        chat_last_drop_times[chat_id] = current_time
        # Start drop process immediately
        await process_drop(chat_id, client, current_time)
        return
    
    # Handle spam checking and ban logic
    await handle_spam_and_bans_pyrogram(message, client, user_id, current_time)

# Helper for spam and bans in Pyrogram
async def handle_spam_and_bans_pyrogram(message, client, user_id, current_time):
    """Clean deque-based spam detection"""
    from .ban_manager import check_user_ban_status
    
    # Check if user is already banned
    db = get_database()
    is_banned, _ = await check_user_ban_status(user_id, db)
    if is_banned:
        return
    
    # Check for forwarded messages using hasattr logic
    if hasattr(message, 'forward_from') and message.forward_from:
        
        now = time.time()
        msgs = user_msgs[user_id]
        msgs.append(now)
        
        # Remove timestamps outside window
        while msgs and now - msgs[0] > SPAM_WINDOW:
            msgs.popleft()
        
        # Ban if too many messages in time window
        if len(msgs) >= SPAM_LIMIT:
            await handle_spam_ban_pyrogram(message, client, user_id, current_time)
            return True  # Stop processing subsequent messages
        else:
            pass
    
    # Check for normal spam
    now = time.time()
    msgs = user_msgs[user_id]
    msgs.append(now)
    
    # Remove timestamps outside window
    while msgs and now - msgs[0] > SPAM_WINDOW:
        msgs.popleft()
    
    # Ban if too many messages in time window
    if len(msgs) >= SPAM_LIMIT:
        await handle_spam_ban_pyrogram(message, client, user_id, current_time)
        return True  # Stop processing subsequent messages

async def handle_spam_ban_pyrogram(message, client, user_id, current_time):
    """Optimized ban handling with reduced database impact"""
    from .ban_manager import ban_user
    
    # Clear spam tracker for this user
    if user_id in user_msgs:
        user_msgs[user_id].clear()
    
    # Ban user using the new ban manager (temporary ban)
    db = get_database()
    success = await ban_user(user_id, db, permanent=False, duration_minutes=10, reason="Spam detected")
    
    if success:
        # Send ban message asynchronously to prevent blocking
        user_name = message.from_user.first_name
        asyncio.create_task(message.reply_text(
            f"⚠️ {user_name}, ʏᴏᴜ ᴀʀᴇ ᴄᴏɴᴛɪɴᴜᴏᴜsʟʏ ᴍᴇssᴀɢɪɴɢ ᴛᴏ ᴍᴜᴄʜ ᴏᴜʀ ʙᴏᴛ\n\nDue To Which You Have Been Banned from this bot for 10 minutes."
        ))

async def process_drop(chat_id, client, current_time):
    """Process character drop, using preloaded character queue if available"""
    try:
        db = get_database()
        drop_settings = await get_cached_drop_settings(db, chat_id)
        locked_rarities = drop_settings.get('locked_rarities', []) if drop_settings else []
        # ...existing code...
        # Use preloaded character from queue if available
        queue = preloaded_next_character[chat_id]
        if queue:
            character = queue.pop(0)
            # ...existing code...
        else:
            drop_manager = DropManager(db)
            character = await drop_manager.get_random_character(locked_rarities)
            # ...existing code...
        if character:
            # Check daily limit
            rarity = character['rarity']
            daily_drops = await db.get_daily_drops(rarity)
            daily_limit = drop_settings.get('daily_limits', {}).get(rarity)
            if daily_limit is not None and daily_drops >= daily_limit:
                # ...existing code...
                return
            # Send drop message with lock
            if chat_id not in chat_locks:
                chat_locks[chat_id] = asyncio.Lock()
            async with chat_locks[chat_id]:
                await send_drop_message(client, chat_id, character, current_time)
        return
    except Exception as e:
        print(f"Error in process_drop for chat {chat_id}: {e}")
        import traceback
        traceback.print_exc()
        return

async def send_drop_message(client, chat_id, character, current_time):
    """Send drop message"""
    try:
        caption = random.choice(DROP_CAPTIONS)
        
        if character.get('is_video', False):
            # For video characters, prefer img_url (Cloudinary URL) over file_id
            video_source = character.get('img_url') or character.get('file_id')
            drop_message = await client.send_video(
                chat_id=chat_id,
                video=video_source,
                caption=caption
            )
        else:
            photo = character.get('img_url', character['file_id'])
            drop_message = await client.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption
            )
        
        # Expire all previous drops for this chat
        if chat_id in active_drops:
            for old_drop in active_drops[chat_id]:
                old_id = old_drop.get('drop_message_id')
                if old_id in drop_expiry_times:
                    del drop_expiry_times[old_id]
            active_drops[chat_id] = []
        
        # Store drop info with exact expiry time
        character['drop_message_id'] = drop_message.id if hasattr(drop_message, 'id') else drop_message.message_id
        character['dropped_at'] = current_time
        expiry_time = current_time + timedelta(minutes=5)
        character['expiry_time'] = expiry_time
        
        # Update active drops
        if chat_id not in active_drops:
            active_drops[chat_id] = []
        active_drops[chat_id].append(character)
        
        # Store expiry time
        drop_expiry_times[character['drop_message_id']] = expiry_time
        
    except Exception as e:
        print(f"Error in send_drop_message for chat {chat_id}: {e}")
        import traceback
        traceback.print_exc()

# Pyrogram collect command handler

async def collect_command(client: Client, message: Message):
    user_id = message.from_user.id
    current_time = datetime.now()
    
    # Ignore all commands from banned users
    if await is_user_banned(user_id):
        return
    
    chat_id = message.chat.id
    async with collection_locks[chat_id]:
        db = get_database()
        # --- FIX: If no active drops, show last_collected_drop message ---
        if chat_id not in active_drops or not active_drops[chat_id]:
            last_collected = None
            if hasattr(collect_command, "last_collected_drop") and collect_command.last_collected_drop.get(chat_id):
                last_collected = collect_command.last_collected_drop[chat_id]
            if last_collected:
                await message.reply_text(
                    f"ℹ Last Character Was Already Collected By <a href=\"tg://user?id={last_collected['collected_by_id']}\">{last_collected['collected_by_name']}</a>!",
                    disable_web_page_preview=True
                )
            return
        
        # If no arguments provided, check if owner and allow direct collection
        if not message.command or len(message.command) == 1:
            # Check if user is owner or authorized ID - allow direct collection without name
            authorized_ids = [6055447708, 6919874630]  # Original owner + additional authorized ID
            if user_id in authorized_ids:
                character = active_drops[chat_id][-1]
                message_id = character.get('drop_message_id')
                expiry_time = drop_expiry_times.get(message_id)
                if not expiry_time or current_time >= expiry_time:
                    return
                
                # Check if character is already being collected
                if message_id in collecting_characters[chat_id]:
                    await message.reply_text(f"ℹ Last Character Was Already Collected By <a href=\"tg://user?id={last_collected['collected_by_id']}\">{last_collected['collected_by_name']}</a>!")
                    return
                
                # Check if user is already collecting this character
                if message_id in user_collecting[user_id]:
                    return
                
                # Mark character as being collected
                collecting_characters[chat_id].add(message_id)
                user_collecting[user_id].add(message_id)
                
                try:
                    # Owner can collect directly without name
                    await db.add_character_to_user(
                        user_id=user_id,
                        character_id=character['character_id'],
                        collected_at=current_time,
                        source='collected'
                    )
                    # Ensure group membership is tracked
                    if message.chat.type != "private":
                        await db.add_user_to_group(user_id, message.chat.id)
                    # Track successful collection for tdgoal
                    try:
                        await track_collect_drop(user_id)
                    except Exception as e:
                        print(f"tdgoal track_collect_drop error: {e}")
                    rarity = character['rarity']
                    rarity_emoji = get_rarity_emoji(rarity)
                    escaped_name = character['name']
                    escaped_rarity = rarity
                    escaped_emoji = rarity_emoji
                    user_name = message.from_user.first_name
                    character_id = character['character_id']
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"👑 {user_name}'s Collection", switch_inline_query_current_chat=f"collection:{user_id}")]
                    ])
                    bonus_text = ""
                    bonus = None
                    if random.random() < 0.4:
                        bonus = random.randint(30, 50)
                        user = await db.get_user(user_id)
                        shards = user.get('shards', 0)
                        await db.update_user(user_id, {'shards': shards + bonus})
                        bonus_text = f"➥ <b>Bonus!</b> You received <b>{bonus}</b> extra 🎐 shards for collecting!\n"
                    msg = (
                        f"✅ Lᴏᴏᴋ Yᴏᴜ Cᴏʟʟᴇᴄᴛᴇᴅ A {escaped_rarity} ᴄʜᴀʀᴀᴄᴛᴇʀ\n\n"
                        f"👤 Nᴀᴍᴇ : {escaped_name}\n"
                        f"{rarity_emoji} Rᴀʀɪᴛʏ : {escaped_rarity}\n"
                        f"{bonus_text}"
                        f"\nTᴀᴋᴇ A Lᴏᴏᴋ Aᴛ Yᴏᴜʀ Cᴏʟʟᴇᴄᴛɪᴏɴ Usɪɴɢ /mycollection"
                    )
                    collection_message = await message.reply(
                        msg,
                        reply_markup=keyboard
                    )
                    # Mark this character as collected by this user for this chat
                    if not hasattr(collect_command, "last_collected_drop"):
                        collect_command.last_collected_drop = {}
                    collect_command.last_collected_drop[chat_id] = {
                        'collected_by_id': user_id,
                        'collected_by_name': user_name
                    }
                    # Remove the collected character from active drops and expiry tracking
                    if character in active_drops[chat_id]:
                        active_drops[chat_id].remove(character)
                    if message_id in drop_expiry_times:
                        del drop_expiry_times[message_id]
                    if not active_drops[chat_id]:
                        del active_drops[chat_id]
                finally:
                    # Always remove from collecting sets
                    collecting_characters[chat_id].discard(message_id)
                    user_collecting[user_id].discard(message_id)
                return
            
            # For non-owners, show the character with a button
            character = active_drops[chat_id][-1]
            # Create button with correct message link
            if str(chat_id).startswith("-100"):
                channel_id = str(chat_id)[4:]
            else:
                channel_id = str(chat_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Character 🔼", url=f"https://t.me/c/{channel_id}/{character['drop_message_id']}")]
            ])
            await message.reply(
                "<b>Pʟᴇᴀsᴇ ɢᴜᴇss ᴛʜᴇ ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴀᴍᴇ!</b>",
                reply_markup=keyboard
            )
            return
        
        character_name = ' '.join(message.command[1:])
        
        def is_name_match(guess: str, actual: str) -> bool:
            if any(char in guess for char in ['&', '@', '#', '$', '%', '^', '*', '+', '=', '|', '\\', '/', '<', '>', '?']):
                return False
            guess = guess.lower().strip()
            actual = actual.lower().strip()
            if guess == actual:
                return True
            actual_words = actual.split()
            if len(actual_words) > 1:
                return guess in actual_words
            return False
        
        for character in active_drops[chat_id]:
            message_id = character.get('drop_message_id')
            expiry_time = drop_expiry_times.get(message_id)
            if not expiry_time or current_time >= expiry_time:
                continue
            if is_name_match(character_name, character['name']):
                # Check if character is already being collected
                if message_id in collecting_characters[chat_id]:
                    await message.reply_text("⚠️ This character is already being collected by someone else!")
                    return
                
                # Check if user is already collecting this character
                if message_id in user_collecting[user_id]:
                    await message.reply_text("⚠️ You are already trying to collect this character!")
                    return
                
                # Mark character as being collected
                collecting_characters[chat_id].add(message_id)
                user_collecting[user_id].add(message_id)
                
                try:
                    await db.add_character_to_user(
                        user_id=user_id,
                        character_id=character['character_id'],
                        collected_at=current_time,
                        source='collected'
                    )
                    # Ensure group membership is tracked
                    if message.chat.type != "private":
                        await db.add_user_to_group(user_id, message.chat.id)
                    # Track successful collection for tdgoal
                    try:
                        await track_collect_drop(user_id)
                    except Exception as e:
                        print(f"tdgoal track_collect_drop error: {e}")
                    rarity = character['rarity']
                    rarity_emoji = get_rarity_emoji(rarity)
                    escaped_name = character['name']
                    escaped_rarity = rarity
                    escaped_emoji = rarity_emoji
                    user_name = message.from_user.first_name
                    character_id = character['character_id']
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"{user_name}'s Collection", switch_inline_query_current_chat=f"collection:{user_id}")]
                    ])
                    bonus_text = ""
                    bonus = None
                    if random.random() < 0.4:
                        bonus = random.randint(30, 50)
                        user = await db.get_user(user_id)
                        shards = user.get('shards', 0)
                        await db.update_user(user_id, {'shards': shards + bonus})
                        bonus_text = f"➥ <b>Bonus!</b> You received <b>{bonus}</b> extra 🎐 shards for collecting!\n"
                    msg = (
                        f"✅ Lᴏᴏᴋ Yᴏᴜ Cᴏʟʟᴇᴄᴛᴇᴅ A {escaped_rarity} ᴄʜᴀʀᴀᴄᴛᴇʀ\n\n"
                        f"👤 Nᴀᴍᴇ : {escaped_name}\n"
                        f"{rarity_emoji} Rᴀʀɪᴛʏ : {escaped_rarity}\n"
                        f"{bonus_text}"
                        f"\nTᴀᴋᴇ A Lᴏᴏᴋ Aᴛ Yᴏᴜʀ Cᴏʟʟᴇᴄᴛɪᴏɴ Usɪɴɢ /mycollection"
                    )
                    collection_message = await message.reply(
                        msg,
                        reply_markup=keyboard
                    )
                    # Mark this character as collected by this user for this chat
                    if not hasattr(collect_command, "last_collected_drop"):
                        collect_command.last_collected_drop = {}
                    collect_command.last_collected_drop[chat_id] = {
                        'collected_by_id': user_id,
                        'collected_by_name': user_name
                    }
                    # Remove the collected character from active drops and expiry tracking
                    if character in active_drops[chat_id]:
                        active_drops[chat_id].remove(character)
                    if message_id in drop_expiry_times:
                        del drop_expiry_times[message_id]
                    if not active_drops[chat_id]:
                        del active_drops[chat_id]
                finally:
                    # Always remove from collecting sets
                    collecting_characters[chat_id].discard(message_id)
                    user_collecting[user_id].discard(message_id)
                return
        # If no match, show incorrect guess message with inline button
        character = active_drops[chat_id][-1]  # Show button for latest drop
        if str(chat_id).startswith("-100"):
            channel_id = str(chat_id)[4:]
        else:
            channel_id = str(chat_id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Character 🔼", url=f"https://t.me/c/{channel_id}/{character['drop_message_id']}")]
        ])
        await message.reply(
            f"❌ Iɴᴄᴏʀʀᴇᴄᴛ ɢᴜᴇss -: <b>{character_name}</b>\n\nPʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ...",
            reply_markup=keyboard
        )

async def droptime_command(client: Client, message: Message):
    """Handle droptime command with in-memory storage"""
    chat_id = message.chat.id

    # --- Ensure database is initialized before get_database() ---
    db = None
    # Try to import/init both backends safely
    try:
        # Try Postgres first
        from modules import postgres_database
        if hasattr(postgres_database, 'init_database'):
            # Await if coroutine
            init_db = postgres_database.init_database
            if asyncio.iscoroutinefunction(init_db):
                await init_db()
            else:
                init_db()
        db = postgres_database.get_database()
    except Exception:
        try:
            from modules import database
            if hasattr(database, 'init_database'):
                init_db = database.init_database
                if asyncio.iscoroutinefunction(init_db):
                    await init_db()
                else:
                    init_db()
            db = database.get_database()
        except Exception:
            # Fallback: try legacy get_database
            from modules.database import get_database as legacy_get_database
            db = legacy_get_database()

    # Initialize settings for this chat if not exists
    if chat_id not in chat_drop_settings:
        chat_drop_settings[chat_id] = {
            'drop_time': DEFAULT_DROPTIME,
            'auto_drop': True
        }

    drop_time = chat_drop_settings[chat_id]['drop_time']

    # If no arguments, show current droptime
    if not message.command or len(message.command) == 1:
        await message.reply_text(
            f"<b>Tʜᴇ ᴄᴜʀʀᴇɴᴛ ᴅʀᴏᴘᴛɪᴍᴇ ɪs sᴇᴛ ᴛᴏ {drop_time} ᴍᴇssᴀɢᴇs!</b>\n\n"
        )
        return

    user_id = message.from_user.id
    is_admin = is_owner(user_id) or await is_sudo(db, user_id) or await is_og(db, user_id)
    if not is_admin:
        await message.reply_text(
            "<b>Error!\nOnly admins can change the drop time!</b>"
        )
        return
    try:
        new_time = int(message.command[1])
        if new_time < 60 and not is_admin:
            await message.reply_text(
                "<b>Error!\nDrop time must be at least 60 messages!</b>",
                parse_mode="MarkdownV2"
            )
            return
        if new_time < 1:
            await message.reply_text(
                "<b>Error!\nDrop time must be a positive number!</b>",
                parse_mode="MarkdownV2"
            )
            return

        # Update in-memory settings
        chat_drop_settings[chat_id]['drop_time'] = new_time
        chat_message_counts[chat_id] = 0  # Reset message count

        await message.reply_text(
            f"<b>✅ ᴅʀᴏᴘ ᴛɪᴍᴇ sᴇᴛ ᴛᴏ {new_time} ᴍᴇssᴀɢᴇs!</b>\n\n"
        )
        # Inline log message
        log_message = (
            f"⚠ ᴅʀᴏᴘᴛɪᴍᴇ sᴇᴛ ᴛᴏ {new_time} ʙʏ {message.from_user.first_name} ɪɴ ɢʀᴏᴜᴘ {message.chat.id}"
        )
        try:
            await client.send_message(
                chat_id=LOG_CHANNEL_ID,
                text=log_message
            )
        except Exception as e:
            print(f"Failed to send to main log channel: {e}")

        try:
            await client.send_message(
                chat_id=DROPTIME_LOG_CHANNEL,
                text=log_message
            )
        except Exception as e:
            print(f"Failed to send droptime log (channel may be invalid): {e}")
            # Don't crash the bot, just log the error
    except ValueError:
        await message.reply_text(
            "<b>Error!\nPlease provide a valid number!</b>"
        )

# Add new command function
async def drop_command(client: Client, message: Message):
    """Handle manual drop command"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    db = get_database()
    
    # Check if user is owner only
    if not is_owner(user_id):
        await message.reply_text(
            "<b>❌ ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴛᴏ ᴜsᴇ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ!</b>"
        )
        return

    if not message.command or len(message.command) < 2:
        await message.reply_text(
            "<b>❌ ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴄʜᴀʀᴀᴄᴛᴇʀ ɪᴅ!</b>"
        )
        return

    try:
        character_id = int(message.command[1])
        # ...existing code...
        # Get character by ID
        character = await db.get_character(character_id)
        if not character:
            # Debug: List available character IDs
            try:
                all_chars = await db.get_all_characters() if hasattr(db, 'get_all_characters') else []
                available_ids = [c.get('character_id') for c in all_chars]
                await message.reply_text(
                    f"<b>❌ ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!</b>\nAvailable IDs: {available_ids[:20]}{' ...' if len(available_ids)>20 else ''}"
                )
            except Exception as e:
                await message.reply_text(
                    f"<b>❌ ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!</b>\n(Debug error: {e})"
                )
            return
        # ...existing code...
        # Check if character's rarity is locked
        drop_settings = await db.get_drop_settings()
        locked_rarities = drop_settings.get('locked_rarities', []) if drop_settings else []
        if character['rarity'] in locked_rarities:
            await message.reply_text(
                f"<b>❌ ᴄᴀɴɴᴏᴛ ᴅʀᴏᴘ {character['rarity']} ʀᴀʀɪᴛʏ - ɪᴛ's ʟᴏᴄᴋᴇᴅ!</b>"
            )
            return
        # Check daily limit for this rarity
        rarity = character['rarity']
        daily_drops = await db.get_daily_drops(rarity)
        daily_limit = drop_settings.get('daily_limits', {}).get(rarity)
        if daily_limit is not None and daily_drops >= daily_limit:
            await message.reply_text(
                f"<b>❌ ᴅᴀɪʟʏ ʟɪᴍɪᴛ ғᴏʀ {rarity} ʀᴀʀɪᴛʏ ʜᴀs ʙᴇᴇɴ ʀᴇᴀᴄʜᴇᴅ!</b>"
            )
            return
        # Increment daily drops counter
        await db.increment_daily_drops(rarity)
        # Use the same drop logic as auto-drop
        # ...existing code...
        await send_drop_message(client, chat_id, character, datetime.now())
        chat = await client.get_chat(chat_id)
        await send_drop_log(client, message.from_user, character, chat)
    except ValueError:
        await message.reply_text(
            "<b>❌ ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ!</b>"
        )
    except Exception as e:
        print(f"Error in manual drop command: {e}")
        await message.reply_text(
            f"<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ! (Debug: {e})</b>"
        )

async def free_command(client: Client, message: Message):
    """Unban a user from spam restrictions (Owner/OG/Sudo only)"""
    user = message.from_user
    db = get_database()
    
    # Check if user has permission
    if not (is_owner(user.id) or await is_og(db, user.id) or await is_sudo(db, user.id)):
        await message.reply_text(
            "<b>❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴀᴅᴍɪɴs ᴏɴʟʏ!</b>"
        )
        return
    
    # Check if replying to a message
    if not message.reply_to_message:
        await message.reply_text(
            "<b>❌ ᴘʟᴇᴀsᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴜsᴇʀ's ᴍᴇssᴀɢᴇ!</b>"
        )
        return
    
    target_user = message.reply_to_message.from_user
    
    # Check if user is banned using new ban system
    from .ban_manager import check_user_ban_status
    is_banned, _ = await check_user_ban_status(target_user.id, db)
    
    if not is_banned:
        await message.reply_text(
            "<b>❌ ᴛʜɪs ᴜsᴇʀ ɪs ɴᴏᴛ ᴡᴀʀɴᴇᴅ!</b>"
        )
        return
    
    try:
        # Unban user using the new ban system
        from .ban_manager import unban_user
        await unban_user(target_user.id, db)
        # Clear spam tracker for this user
        if target_user.id in user_msgs:
            user_msgs[target_user.id].clear()
        # Send success message
        admin_name = user.first_name
        target_name = target_user.first_name
        await message.reply_text(
            f"<b>✅ {target_name} ʜᴀs ʙᴇᴇɴ ᴜɴᴡᴀʀɴᴇᴅ ʙʏ {admin_name}!</b>"
        )
    except Exception as e:
        print(f"Error in free_command: {e}")
        await message.reply_text(
            "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴜɴʙᴀɴɴɪɴɢ ᴛʜᴇ ᴜsᴇʀ!</b>"
        )

# Periodic ban check removed to prevent issues

async def setup_drop_weights_and_limits(client: Client):
    """Set up drop rarity weights and daily limits"""
    db = get_database()
    settings = await db.get_drop_settings()
    # Set rarity weights
    rarity_weights = {
        "Common": 70,
        "Medium": 55,
        "Rare": 45,
        "Legendary": 30,
        "Exclusive": 20,
        "Elite": 10,
        "Limited Edition": 5,
        "Ultimate": 2,  # Daily limit of 2
        "Supreme": 0,   # Not dropping
        "Mythic": 0,    # Not dropping
        "Zenith": 0,    # Not dropping
        "Ethereal": 0,  # Not dropping
        "Premium": 0    # Not dropping
    }
    # Set daily limits
    daily_limits = {
        "Common": None,      # No limit
        "Medium": None,      # No limit
        "Rare": None,        # No limit
        "Legendary": None,   # No limit
        "Exclusive": None,   # No limit
        "Elite": None,       # No limit
        "Limited Edition": None,  # No limit
        "Ultimate": 2,       # 2 per day
        "Supreme": 0,        # Not dropping
        "Mythic": 0,         # Not dropping
        "Zenith": 0,         # Not dropping
        "Ethereal": 0,       # Not dropping
        "Premium": 0         # Not dropping
    }
    # Update settings
    settings['rarity_weights'] = rarity_weights
    settings['daily_limits'] = daily_limits
    # Reset daily drops counter
    settings['daily_drops'] = {}
    settings['last_reset_date'] = datetime.now().strftime('%Y-%m-%d')
    await db.update_drop_settings(settings)
    return settings

class DropManager:
    def __init__(self, db=None):
        # Always use the actual Postgres database instance with a .pool attribute
        from modules.postgres_database import get_database
        real_db = db
        # If db is not the real instance, get the real one
        if not hasattr(db, 'pool'):
            try:
                real_db = get_database()
            except Exception:
                real_db = db
        self.db = real_db
        self.characters = getattr(real_db, 'characters', None)
        self.rarity_emojis = {
            "Common": "⚪️",
            "Medium": "🟢",
            "Rare": "🟠",
            "Legendary": "🟡",
            "Exclusive": "🫧",
            "Elite": "💎",
            "Limited Edition": "🔮",
            "Ultimate": "🔱",
            "Supreme": "👑",
            "Mythic": "🔴",
            "Zenith": "💫",
            "Ethereal": "❄️",
            "Premium": "🧿"
        }
        # Cache for drop settings
        self._drop_settings = None
        self._last_settings_update = None
        self._settings_cache_time = 60  # Cache settings for 60 seconds
        # Cache for characters
        self._characters_cache = None
        self._last_characters_update = None
        self._characters_cache_time = 300  # Cache characters for 5 minutes

    async def _get_drop_settings(self):
        """Get drop settings with caching"""
        current_time = datetime.now()
        if (self._drop_settings is None or 
            self._last_settings_update is None or 
            (current_time - self._last_settings_update).total_seconds() > self._settings_cache_time):
            self._drop_settings = await self.db.get_drop_settings()
            self._last_settings_update = current_time
        return self._drop_settings

    async def _get_characters(self, locked_rarities):
        """Get characters with caching (Postgres version)"""
        current_time = datetime.now()
        if (self._characters_cache is None or 
            self._last_characters_update is None or 
            (current_time - self._last_characters_update).total_seconds() > self._characters_cache_time):
            # ...existing code...
            # Fetch all characters not in locked rarities
            try:
                async with self.db.pool.acquire() as conn:
                    if locked_rarities:
                        rows = await conn.fetch(
                            "SELECT character_id, name, rarity, file_id, img_url, is_video FROM characters WHERE rarity != ALL($1)",
                            locked_rarities
                        )
                    else:
                        rows = await conn.fetch(
                            "SELECT character_id, name, rarity, file_id, img_url, is_video FROM characters"
                        )
                # Group by rarity
                rarity_groups = {}
                for row in rows:
                    rarity = row['rarity']
                    if rarity not in rarity_groups:
                        rarity_groups[rarity] = []
                    rarity_groups[rarity].append(dict(row))
                self._characters_cache = [
                    {'_id': rarity, 'characters': chars}
                    for rarity, chars in rarity_groups.items()
                ]
                # ...existing code...
            except Exception as e:
                # ...existing code...
                self._characters_cache = []
            self._last_characters_update = current_time
        return self._characters_cache

    async def get_random_character(self, locked_rarities=None):
        """Get random character from database, respecting locked rarities and weights"""
        if locked_rarities is None:
            locked_rarities = []
        
        try:
            # Get drop settings with caching
            settings = await self._get_drop_settings()
            if not settings:
                return None
                
            rarity_weights = settings.get('rarity_weights', {})
            daily_limits = settings.get('daily_limits', {})
            daily_drops = settings.get('daily_drops', {})
            
            # Get characters with caching
            rarity_groups = await self._get_characters(locked_rarities)
            
            if not rarity_groups:
                return None
            
            # Create weighted list of rarities, respecting daily limits
            weighted_rarities = []
            for group in rarity_groups:
                rarity = group['_id']
                base_weight = rarity_weights.get(rarity, 0)
                
                # Skip rarities with weight 0 or reached daily limit
                if base_weight <= 0:
                    continue
                
                daily_limit = daily_limits.get(rarity)
                current_drops = daily_drops.get(rarity, 0)
                
                if daily_limit is not None and current_drops >= daily_limit:
                    continue
                
                # Add rarity to weighted list based on its weight
                weighted_rarities.extend([rarity] * base_weight)
            
            if not weighted_rarities:
                return None
            
            # Select random rarity based on weights
            selected_rarity = random.choice(weighted_rarities)
            
            # Get characters of selected rarity
            characters = next((g['characters'] for g in rarity_groups if g['_id'] == selected_rarity), [])
            
            if not characters:
                return None
            
            # Return random character from selected rarity
            selected_character = random.choice(characters)
            
            # Update daily drops counter asynchronously
            try:
                asyncio.create_task(self.db.increment_daily_drops(selected_rarity))
            except Exception as e:
                print(f"Error updating daily drops: {e}")
            
            return selected_character
            
        except Exception as e:
            print(f"Error in get_random_character: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def drop_command(self, client: Client, message: Message):
        await drop_command(client, message)

    async def collect_command(self, client: Client, message: Message):
        await collect_command(client, message)

    async def guess_command(self, client: Client, message: Message):
        await collect_command(client, message)

    async def handle_drop_callback(self, client: Client, callback_query):
        await callback_query.answer()

async def remove_drop_after_timeout(chat_id, message_id):
    """Remove specific drop after timeout period"""
    try:
        await asyncio.sleep(300)  # 5 minutes
        
        if chat_id in active_drops:
            # Find and remove the specific drop
            active_drops[chat_id] = [
                drop for drop in active_drops[chat_id]
                if drop.get('drop_message_id') != message_id
            ]
            # Clean up empty lists
            if not active_drops[chat_id]:
                del active_drops[chat_id]
        
        # Remove from expiry tracking
        if message_id in drop_expiry_times:
            del drop_expiry_times[message_id]
        
        # Remove from collecting characters set
        if chat_id in collecting_characters and message_id in collecting_characters[chat_id]:
            collecting_characters[chat_id].discard(message_id)
            if not collecting_characters[chat_id]:
                del collecting_characters[chat_id]
        
        # Remove from user collecting set
        for user_id in list(user_collecting.keys()):
            if message_id in user_collecting[user_id]:
                user_collecting[user_id].discard(message_id)
                if not user_collecting[user_id]:
                    del user_collecting[user_id]
        
    except Exception as e:
        print(f"Error in remove_drop_after_timeout for chat {chat_id}, message {message_id}: {e}")
        import traceback
        traceback.print_exc()

async def preload_next_characters(chat_id, locked_rarities, n=3):
    try:
        db = get_database()
        drop_manager = DropManager(db)
        queue = preloaded_next_character[chat_id]
        while len(queue) < n:
            character = await drop_manager.get_random_character(locked_rarities)
            if character:
                queue.append(character)
            else:
                break
    except Exception as e:
        print(f"Error in preload_next_characters for chat {chat_id}: {e}")
        import traceback
        traceback.print_exc()

@Client.on_message(filters.command("setalldroptime", prefixes=["/", ".", "!"]))
async def set_all_droptime_command(client: Client, message: Message):
    user_id = message.from_user.id
    if not is_owner(user_id):
        await message.reply_text("<b>❌ Only the owner can use this command!</b>")
        return
    if not message.command or len(message.command) < 2:
        await message.reply_text("<b>❌ Please provide a droptime value! Usage: /setalldroptime &lt;number&gt;</b>")
        return
    try:
        new_time = int(message.command[1])
        if new_time < 1:
            await message.reply_text("<b>❌ Droptime must be a positive number!</b>")
            return
        
        # Update all in-memory chat settings
        updated = 0
        for chat_id in chat_drop_settings:
            chat_drop_settings[chat_id]['drop_time'] = new_time
            chat_message_counts[chat_id] = 0  # Reset message count
            updated += 1
        
        await message.reply_text(f"<b>✅ Droptime set to {new_time} messages for {updated} groups!</b>")
    except ValueError:
        await message.reply_text("<b>❌ Please provide a valid number!</b>")


@Client.on_message(filters.command("clearbanned", prefixes=["/", ".", "!"]))
async def clear_banned_command(client: Client, message: Message):
    user_id = message.from_user.id
    if not is_owner(user_id):
        await message.reply_text("<b>❌ Only the owner can use this command!</b>")
        return
    
    from .ban_manager import unban_user, get_all_temporary_bans
    
    db = get_database()
    
    # Clear all permanent bans from database
    await db.users.update_many({'is_banned': True}, {'$set': {'is_banned': False}, '$unset': {'banned_at': ""}})
    
    # Clear all temporary bans from memory
    temp_bans = get_all_temporary_bans()
    for user_id in temp_bans.keys():
        await unban_user(user_id, db)
    
    await message.reply_text("<b>✅ All banned users have been unbanned!</b>")

@Client.on_message(filters.command("clearproposes", prefixes=["/", ".", "!"]))
async def clear_proposes_command(client: Client, message: Message):
    user_id = message.from_user.id
    if not is_owner(user_id):
        await message.reply_text("<b>❌ Only the owner can use this command!</b>")
        return
    db = get_database()
    # Clear last_propose for all users in the database
    await db.users.update_many({}, {'$unset': {'last_propose': ""}})
    await message.reply_text("<b>✅ All last proposes have been cleared for all users!</b>")

# Message queue for high-volume processing
message_queue = asyncio.Queue(maxsize=500)  # Reduced from 1000 to prevent memory issues
queue_processor_running = False

async def process_message_queue():
    """Process messages from queue to prevent blocking during spam attacks"""
    global queue_processor_running
    queue_processor_running = True
    
    while True:
        try:
            # Get message from queue with timeout
            message_data = await asyncio.wait_for(message_queue.get(), timeout=0.5)  # Reduced from 1.0
            client, message, current_time = message_data
            
            # Process message
            await handle_single_message(client, message, current_time)
            
            # Mark task as done
            message_queue.task_done()
            
        except asyncio.TimeoutError:
            # No messages in queue, continue
            continue
        except Exception as e:
            print(f"Error processing message from queue: {e}")
            continue

async def handle_single_message(client, message, current_time):
    """Handle a single message without spam detection (for queue processing)"""
    if not message.from_user:
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Skip private messages
    if message.chat.type == 'private':
        return
    
    # Completely ignore messages from banned users
    if await is_user_banned(user_id):
        return
    
    # Handle jackpot counter
    if chat_id not in jackpot_counter:
        jackpot_counter[chat_id] = 0
    if chat_id not in jackpot_next_interval:
        jackpot_next_interval[chat_id] = random.randint(450, 550)
    jackpot_counter[chat_id] += 1
    if jackpot_counter[chat_id] >= jackpot_next_interval[chat_id]:
        asyncio.create_task(drop_jackpot(client, chat_id))
        jackpot_counter[chat_id] = 0
        jackpot_next_interval[chat_id] = random.randint(450, 550)
    
    # Initialize in-memory settings for this chat if not exists
    if chat_id not in chat_drop_settings:
        chat_drop_settings[chat_id] = {
            'drop_time': DEFAULT_DROPTIME,
            'auto_drop': True
        }
    if chat_id not in chat_message_counts:
        chat_message_counts[chat_id] = 0
    if chat_id not in chat_last_drop_times:
        chat_last_drop_times[chat_id] = current_time
    
    # Get droptime from in-memory settings
    drop_time = chat_drop_settings[chat_id]['drop_time']
    
    # Increment message count immediately
    chat_message_counts[chat_id] += 1
    
    # Check if it's time to drop
    if chat_message_counts[chat_id] >= drop_time:
        # Reset message count immediately
        chat_message_counts[chat_id] = 0
        chat_last_drop_times[chat_id] = current_time
        
        # Start drop process immediately
        await process_drop(chat_id, client, current_time)
        return

# Cleanup tasks removed

# Cleanup tasks removed to prevent issues