from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatType
from datetime import datetime, timedelta
import pytz
from .decorators import is_owner, is_og, check_banned
from config import OWNER_ID
import os

# Import database based on configuration
from modules.postgres_database import get_database
import time

# Helper for markdown v2 escaping (minimal)
def escape_markdown(text, version=2):
    if not text:
        return ''
    return str(text).replace('_', '\_').replace('*', '\*').replace('[', '\[').replace('`', '\`')

# Reward amounts for top collectors (in Grab Tokens)
REWARDS = {
    1: 400000,  # 1st place: 400K tokens
    2: 375000,  # 2nd place: 375K tokens
    3: 350000,  # 3rd place: 350K tokens
    4: 325000,  # 4th place: 325K tokens
    5: 300000,  # 5th place: 300K tokens
    6: 275000,  # 6th place: 275K tokens
    7: 250000,  # 7th place: 250K tokens
    8: 225000,  # 8th place: 225K tokens
    9: 200000,  # 9th place: 200K tokens
    10: 175000  # 10th place: 175K tokens
}

# Simple in-memory cache for leaderboard results
_leaderboard_cache = {}
_CACHE_TTL = 60  # seconds

def get_cached_leaderboard(key):
    entry = _leaderboard_cache.get(key)
    if entry and time.time() - entry['time'] < _CACHE_TTL:
        return entry['data']
    return None

def set_cached_leaderboard(key, data):
    _leaderboard_cache[key] = {'data': data, 'time': time.time()}

async def distribute_daily_rewards(client: Client):
    """Distribute rewards to top collectors at 11 PM IST"""
    try:
        db = get_database()
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        collectors = []
        async for user in db.users.find({}):
            user_id = user['user_id']
            collection_history = user.get('collection_history', [])
            today_count = sum(
                1 for entry in collection_history 
                if isinstance(entry.get('collected_at'), datetime) 
                and today <= entry['collected_at'] < tomorrow
                and entry.get('source', 'collected') == 'collected'
            )
            if today_count > 0:
                collectors.append({
                    'first_name': user.get('first_name', 'Unknown'),
                    'user_id': user_id,
                    'count': today_count
                })
        top_collectors = sorted(collectors, key=lambda x: x['count'], reverse=True)[:10]
        if not top_collectors:
            return
        for idx, collector in enumerate(top_collectors, 1):
            if idx in REWARDS:
                reward = REWARDS[idx]
                await db.users.update_one(
                    {'user_id': collector['user_id']},
                    {'$inc': {'wallet': reward}}
                )
        message = "🎉 <b>Daily Leaderboard Results</b> 🎉\n\n<b>Top Collectors of the Day:</b>\n\n"
        for idx, collector in enumerate(top_collectors, 1):
            if idx in REWARDS:
                reward = REWARDS[idx]
                user_link = f"tg://user?id={collector['user_id']}"
                escaped_name = escape_markdown(collector['first_name'], version=2)
                message += (
                    f"🏅 <b>{idx}</b> Place: <a href='{user_link}'>{escaped_name}</a> <b>➣ {reward:,} Grab Tokens</b>\n"
                )
        message += "\n<b>Congratulations to the winners!</b> 🎊\nYour rewards have been added to your balances!"
        try:
            sent_message = await client.send_message(
                chat_id=-1002585831452,
                text=message,
                disable_web_page_preview=True
            )
            await client.pin_chat_message(
                chat_id=-1002585831452,
                message_id=sent_message.message_id,
                disable_notification=True
            )
        except Exception as e:
            print(f"Error sending/pinning message: {e}")
    except Exception as e:
        print(f"Error in distribute_daily_rewards: {e}")

@check_banned
async def tdtop_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Today's Leaderboard Details")
    try:
        db = get_database()
        ist = pytz.timezone('Asia/Kolkata')
        from datetime import datetime, timedelta
        import pytz
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
        now_ist = now_utc.astimezone(ist)
        today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_ist = today_ist + timedelta(days=1)
        collectors = []
        cursor = await db.users.find({})
        users = await cursor.to_list(length=None)
        import json
        for user in users:
            user_id = user['user_id']
            collection_history = user.get('collection_history', [])
            if isinstance(collection_history, str):
                try:
                    collection_history = json.loads(collection_history)
                except Exception:
                    collection_history = []
            if collection_history is None:
                collection_history = []
            today_count = 0
            debug_entries = []
            for entry in collection_history:
                try:
                    collected_at = entry.get('collected_at')
                    if not collected_at:
                        continue
                    from dateutil.parser import parse
                    collected_at_dt = parse(collected_at)
                    # Always treat as UTC if naive
                    if collected_at_dt.tzinfo is None:
                        collected_at_dt = collected_at_dt.replace(tzinfo=pytz.UTC)
                    collected_at_ist = collected_at_dt.astimezone(ist)
                    if today_ist <= collected_at_ist < tomorrow_ist and entry.get('source', 'collected') == 'collected':
                        today_count += 1
                        debug_entries.append(entry)
                except Exception as e:
                    continue
            if today_count > 0:
                print(f"[DEBUG] User {user_id} counted {today_count} collections today. Entries:", debug_entries)
                collectors.append({
                    'first_name': user.get('first_name', 'Unknown'),
                    'user_id': user_id,
                    'count': today_count
                })
            else:
                continue
        top_collectors = sorted(collectors, key=lambda x: x['count'], reverse=True)[:10]
        if not top_collectors:
            await fetching_msg.delete()
            time_remaining = tomorrow_ist - now_ist
            hours = time_remaining.seconds // 3600
            minutes = (time_remaining.seconds % 3600) // 60
            await client.send_message(
                message.chat.id,
                f"<b>❌ ɴᴏ ᴄᴏʟʟᴇᴄᴛᴏʀs ғᴏᴜɴᴅ ғᴏʀ ᴛᴏᴅᴀʏ!</b>\n\n"
                f"<b>⏰ ɴᴇxᴛ ʀᴇsᴇᴛ ɪɴ:</b> <code>{hours}h {minutes}m</code>"
            )
            return
        time_remaining = tomorrow_ist - now_ist
        hours = time_remaining.seconds // 3600
        minutes = (time_remaining.seconds % 3600) // 60
        message_text = (
            "🌟 <b>Today's Top 10 Collectors</b> 🌟\n\n"
            f"<b>⏰ ɴᴇxᴛ ʀᴇsᴇᴛ ɪɴ:</b> <code>{hours}h {minutes}m</code>\n\n"
        )
        medals = ["🥇", "🥈", "🥉"]
        for idx, collector in enumerate(top_collectors, 1):
            user_link = f"tg://user?id={collector['user_id']}"
            escaped_name = escape_markdown(collector['first_name'], version=2)
            if idx <= 3:
                message_text += f"{medals[idx-1]} <a href='{user_link}'>{escaped_name}</a> ➣ <b>{collector['count']} Collected</b>\n"
            else:
                message_text += f"{idx}. <a href='{user_link}'>{escaped_name}</a> ➣ <b>{collector['count']} Collected</b>\n"
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            message_text,
            disable_web_page_preview=True
        )
    except Exception as e:
        import traceback
        print(f"Error in tdtop_command: {e}")
        print(traceback.format_exc())
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            f"<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ!</b>\n<code>{e}</code>"
        )

@check_banned
async def gtop_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Global Leaderboard Details")
    db = get_database()
    cache_key = 'gtop'
    cached = get_cached_leaderboard(cache_key)
    if cached:
        await fetching_msg.delete()
        await client.send_message(message.chat.id, cached, disable_web_page_preview=True)
        return
    try:
        collectors = []
        cursor = await db.users.find({})
        users = await cursor.to_list(length=None)
        for user in users:
            try:
                characters = user.get('characters', [])
                if characters:
                    unique_chars = len(set(characters))
                    total_chars = len(characters)
                    collectors.append({
                        'first_name': user.get('first_name', 'Unknown'),
                        'user_id': user['user_id'],
                        'total_count': total_chars,
                        'unique_count': unique_chars
                    })
            except Exception:
                continue
        top_collectors = sorted(collectors, key=lambda x: x['total_count'], reverse=True)[:10]
        if not top_collectors:
            await fetching_msg.delete()
            await client.send_message(
                message.chat.id,
                "<b>❌ ɴᴏ ᴄᴏʟʟᴇᴛᴏʀs ғᴏᴜɴᴅ!</b>"
            )
            return
        message_text = "<b>🌍 ɢʟᴏʙᴀʟ ᴛᴏᴘ 10 ᴄᴏʟʟᴇᴄᴛᴏʀs 🌍</b>\n\n"
        medals = ["🥇", "🥈", "🥉"]
        for idx, collector in enumerate(top_collectors, 1):
            user_link = f"tg://user?id={collector['user_id']}"
            escaped_name = escape_markdown(collector['first_name'], version=2)
            prefix = medals[idx-1] if idx <= 3 else f"{idx}"
            message_text += (
                f"<b>{prefix}</b> <a href='{user_link}'>{escaped_name}</a> ➣ <b>{collector['total_count']} | ({collector['unique_count']})</b>\n"
            )
        set_cached_leaderboard(cache_key, message_text)
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            message_text,
            disable_web_page_preview=True
        )
    except Exception as e:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ!</b>"
        )

@check_banned
async def top_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Group Leaderboard Details")
    db = get_database()
    chat = message.chat
    if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ᴄᴀɴ ᴏɴʟʏ ʙᴇ ᴜsᴇᴅ ɪɴ ɢʀᴏᴜᴘs!</b>"
        )
        return
    try:
        # First, ensure the current user is added to this group
        await db.add_user_to_group(message.from_user.id, chat.id)
        
        # Get all users who are members of this specific group
        cursor = await db.users.find({"groups": chat.id})
        group_users = await cursor.to_list(length=None)

        # Get actual current group members from Telegram
        current_members = set()
        try:
            async for member in client.get_chat_members(chat.id):
                if hasattr(member, 'user') and hasattr(member.user, 'id'):
                    current_members.add(member.user.id)
        except Exception as e:
            # If we can't fetch members, fallback to old behavior
            current_members = None

        # Filter group_users to only those who are still in the group
        if current_members is not None:
            group_users = [user for user in group_users if user.get('user_id') in current_members]
        
        if not group_users:
            await fetching_msg.delete()
            await client.send_message(
                message.chat.id,
                "<b>❌ ɴᴏ ᴄᴏʟʟᴇᴄᴛᴏʀs ғᴏᴜɴᴅ ɪɴ ᴛʜɪs ɢʀᴏᴜᴘ!</b>"
            )
            return
        
        # Process users and their collections
        collectors = []
        for user in group_users:
            characters = user.get('characters', [])
            if characters:
                unique_chars = len(set(characters))
                total_chars = len(characters)
                collectors.append({
                    'first_name': user.get('first_name', 'Unknown'),
                    'user_id': user['user_id'],
                    'total_count': total_chars,
                    'unique_count': unique_chars
                })
        
        # Sort by total characters collected and take top 10
        top_collectors = sorted(collectors, key=lambda x: x['total_count'], reverse=True)[:10]
        
        if not top_collectors:
            await fetching_msg.delete()
            await client.send_message(
                message.chat.id,
                "<b>❌ ɴᴏ ᴄᴏʟʟᴇᴄᴛᴏʀs ғᴏᴜɴᴅ ɪɴ ᴛʜɪs ɢʀᴏᴜᴘ!</b>"
            )
            return
        
        message_text = f"<b>📊 ᴛᴏᴘ 10 ᴄᴏʟʟᴇᴄᴛᴏʀs ɪɴ {escape_markdown(chat.title, version=2)} 📊</b>\n\n"
        medals = ["🥇", "🥈", "🥉"]
        for idx, collector in enumerate(top_collectors, 1):
            user_link = f"tg://user?id={collector['user_id']}"
            escaped_name = escape_markdown(collector['first_name'], version=2)
            prefix = medals[idx-1] if idx <= 3 else f"{idx}"
            message_text += (
                f"<b>{prefix}</b> <a href='{user_link}'>{escaped_name}</a> ➣ <b>{collector['total_count']} | ({collector['unique_count']})</b>\n"
            )
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            message_text,
            disable_web_page_preview=True
        )
    except Exception as e:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ғᴇᴛᴄʜɪɴɢ ᴛᴏᴘ ᴄᴏʟʟᴇᴄᴛᴏʀs!</b>"
        )

@check_banned
async def rgtop_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Richest Collectors Leaderboard")
    db = get_database()
    rich_users = []
    cursor = await db.users.find({})
    users = await cursor.to_list(length=None)
    for user in users:
        wallet = user.get('wallet', 0)
        if wallet > 0:
            rich_users.append({
                'first_name': user['first_name'],
                'user_id': user['user_id'],
                'wallet': wallet
            })
    top_rich = sorted(rich_users, key=lambda x: x['wallet'], reverse=True)[:10]
    if not top_rich:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ɴᴏ ᴜsᴇʀs ғᴏᴜɴᴅ!</b>"
        )
        return
    message_text = "<b>💰 ɢʟᴏʙᴀʟ ᴛᴏᴘ 10 ʀɪᴄʜᴇsᴛ ᴄᴏʟʟᴇᴄᴛᴏʀs 💰</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for idx, user in enumerate(top_rich, 1):
        user_link = f"tg://user?id={user['user_id']}"
        escaped_name = escape_markdown(user['first_name'], version=2)
        prefix = medals[idx-1] if idx <= 3 else f"{idx}"
        wallet = f"{user['wallet']:,}"
        message_text += (
            f"<b>{prefix}</b> <a href='{user_link}'>{escaped_name}</a> ➣ <b>{wallet} Tokens</b>\n"
        )
    await fetching_msg.delete()
    await client.send_message(
        message.chat.id,
        message_text,
        disable_web_page_preview=True
    )

@check_banned
async def btop_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Top Bank Balances")
    db = get_database()
    user_id = message.from_user.id
    if not (is_owner(user_id) or await is_og(db, user_id)):
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴏᴡɴᴇʀ ᴀɴᴅ ᴏɢs ᴏɴʟʏ!</b>"
        )
        return
    bank_users = []
    cursor = await db.users.find({})
    users = await cursor.to_list(length=None)
    for user in users:
        bank = user.get('bank') or 0
        if bank > 0:
            bank_users.append({
                'first_name': user['first_name'],
                'user_id': user['user_id'],
                'bank': bank
            })
    top_bank = sorted(bank_users, key=lambda x: x['bank'], reverse=True)[:25]
    if not top_bank:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ɴᴏ ᴜsᴇʀs ᴡɪᴛʜ ʙᴀɴᴋ ʙᴀʟᴀɴᴄᴇ ғᴏᴜɴᴅ!</b>"
        )
        return
    message_text = "<b>🏦 ᴛᴏᴘ 25 ʙᴀɴᴋ ʙᴀʟᴀɴᴄᴇs 🏦</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for idx, user in enumerate(top_bank, 1):
        user_link = f"tg://user?id={user['user_id']}"
        escaped_name = escape_markdown(user['first_name'], version=2)
        prefix = medals[idx-1] if idx <= 3 else f"{idx}"
        bank = f"{user['bank']:,}"
        message_text += (
            f"<b>{prefix}</b> <a href='{user_link}'>{escaped_name}</a> ➣ <b>{bank} Tokens</b>\n"
        )
    await fetching_msg.delete()
    await client.send_message(
        message.chat.id,
        message_text,
        disable_web_page_preview=True
    )

@check_banned
async def sgtop_command(client: Client, message: Message):
    # Show fetching message
    fetching_msg = await client.send_message(message.chat.id, "🔄 Fetching Top Shards Collectors Leaderboard")
    db = get_database()
    shard_users = []
    cursor = await db.users.find({})
    users = await cursor.to_list(length=None)
    for user in users:
        shards = user.get('shards', 0)
        if shards > 0:
            shard_users.append({
                'first_name': user['first_name'],
                'user_id': user['user_id'],
                'shards': shards
            })
    top_shards = sorted(shard_users, key=lambda x: x['shards'], reverse=True)[:10]
    if not top_shards:
        await fetching_msg.delete()
        await client.send_message(
            message.chat.id,
            "<b>❌ ɴᴏ ᴜsᴇʀs ғᴏᴜɴᴅ!</b>"
        )
        return
    message_text = "<b>🎐 ɢʟᴏʙᴀʟ ᴛᴏᴘ 10 sʜᴀʀᴅ ᴄᴏʟʟᴇᴄᴛᴏʀs 🎐</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for idx, user in enumerate(top_shards, 1):
        user_link = f"tg://user?id={user['user_id']}"
        escaped_name = escape_markdown(user['first_name'], version=2)
        prefix = medals[idx-1] if idx <= 3 else f"{idx}"
        shards = f"{user['shards']:,}"
        message_text += (
            f"<b>{prefix}</b> <a href='{user_link}'>{escaped_name}</a> ➣ <b>{shards} Shards</b>\n"
        )
    await fetching_msg.delete()
    await client.send_message(
        message.chat.id,
        message_text,
        disable_web_page_preview=True
    )

@check_banned
async def test_leaderboard_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id != OWNER_ID:
        await message.reply_text(
            "<b>❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴛʜᴇ ʙᴏᴛ ᴏᴡɴᴇʀ!</b>"
        )
        return
    try:
        db = get_database()
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        collectors = []
        cursor = await db.users.find({})
        users = await cursor.to_list(length=None)
        for user in users:
            user_id = user['user_id']
            collection_history = user.get('collection_history', [])
            today_count = sum(
                1 for entry in collection_history 
                if isinstance(entry.get('collected_at'), datetime) 
                and (
                    (entry['collected_at'].tzinfo is None and 
                     today <= entry['collected_at'].replace(tzinfo=ist) < tomorrow)
                    or
                    (entry['collected_at'].tzinfo is not None and 
                     today <= entry['collected_at'].astimezone(ist) < tomorrow)
                )
                and entry.get('source', 'collected') == 'collected'
            )
            if today_count > 0:
                collectors.append({
                    'first_name': user.get('first_name', 'Unknown'),
                    'user_id': user_id,
                    'count': today_count
                })
        top_collectors = sorted(collectors, key=lambda x: x['count'], reverse=True)[:10]
        if not top_collectors:
            await message.reply_text(
                "<b>❌ ɴᴏ ᴄᴏʟʟᴇᴄᴛᴏʀs ғᴏᴜɴᴅ ғᴏʀ ᴛᴏᴅᴀʏ!</b>",
            )
            return
        for idx, collector in enumerate(top_collectors, 1):
            if idx in REWARDS:
                reward = REWARDS[idx]
                await db.users.update_one(
                    {'user_id': collector['user_id']},
                    {'$inc': {'wallet': reward}}
                )
        message_text = "🎉 <b>Daily Leaderboard Results</b> 🎉\n\n<b>Top Collectors of the Day:</b>\n\n"
        for idx, collector in enumerate(top_collectors, 1):
            if idx in REWARDS:
                reward = REWARDS[idx]
                user_link = f"tg://user?id={collector['user_id']}"
                escaped_name = escape_markdown(collector['first_name'], version=2)
                message_text += (
                    f"🏅 <b>{idx}</b> Place: <a href='{user_link}'>{escaped_name}</a> <b>➣ {reward:,} Grab Tokens</b>\n"
                )
        message_text += "\n<b>Congratulations to the winners!</b> 🎊\nYour rewards have been added to your balances!"
        try:
            sent_message = await client.send_message(
                chat_id=-1002585831452,
                text=message_text,
                disable_web_page_preview=True
            )
            await client.pin_chat_message(
                chat_id=-1002585831452,
                message_id=sent_message.message_id,
                disable_notification=True
            )
            await message.reply_text(
                "<b>✅ ᴛᴇsᴛ ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ ᴅɪsᴛʀɪʙᴜᴛɪᴏɴ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!</b>"
            )
        except Exception as e:
            print(f"Error sending/pinning message: {e}")
            await message.reply_text(
                "<b>❌ ᴇʀʀᴏʀ sᴇɴᴅɪɴɢ/pɪɴɴɪɴɢ ᴍᴇssᴀɢᴇ!</b>"
            )
    except Exception as e:
        print(f"Error in test leaderboard: {e}")
        await message.reply_text(
            "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴛᴇsᴛɪɴɢ ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ!</b>"
        )

def setup_top_handlers(app: Client):
    app.add_handler(filters.command("tdtop")(tdtop_command))
    app.add_handler(filters.command("gtop")(gtop_command))
    app.add_handler(filters.command("top")(top_command))
    app.add_handler(filters.command("rgtop")(rgtop_command))
    app.add_handler(filters.command("btop")(btop_command))
    app.add_handler(filters.command("testleaderboard")(test_leaderboard_command))
    app.add_handler(filters.command("sgtop")(sgtop_command))