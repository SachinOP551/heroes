from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from .decorators import owner_only, admin_only, is_owner, is_og
from config import OWNER_ID, DATABASE_NAME
import os

from modules.postgres_database import get_database
from collections import Counter
from datetime import datetime
import logging
from modules.drop_weights import setup_drop_weights_and_limits
import random
from modules.collection import batch_fetch_characters
from pyrogram.enums import ChatType
from modules.logging_utils import send_admin_log
import subprocess
import shutil
import os
import pymongo
import json
from modules.postgres_database import get_rarity_emoji
import asyncio

# Setup logging
admin_logger = logging.getLogger('admin_actions')
handler = logging.FileHandler('admin_actions.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
admin_logger.addHandler(handler)
admin_logger.setLevel(logging.INFO)

BACKUP_CHANNEL_ID = -1002515226068

@owner_only
async def sudo_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Please reply to a user's message to make them sudo!")
        return
    target_user = message.reply_to_message.from_user
    user_data = {
        'user_id': target_user.id,
        'username': target_user.username,
        'first_name': target_user.first_name,
        'sudo': True,
        'og': False
    }
    db = get_database()
    await db.update_user(target_user.id, user_data)
    await send_admin_log(client, message.from_user, "Made user sudo", target=f"{target_user.first_name} ({target_user.id})")
    await message.reply_text(f"Successfully made {target_user.first_name} a sudo admin!")

@owner_only
async def og_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Please reply to a user's message to make them OG!")
        return
    target_user = message.reply_to_message.from_user
    user_data = {
        'user_id': target_user.id,
        'username': target_user.username,
        'first_name': target_user.first_name,
        'sudo': False,
        'og': True
    }
    db = get_database()
    await db.update_user(target_user.id, user_data)
    await message.reply_text(f"Successfully made {target_user.first_name} an OG admin!")

@owner_only
async def remove_sudo_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Please reply to a user's message to remove sudo!")
        return
    target_user = message.reply_to_message.from_user
    db = get_database()
    await db.remove_sudo(target_user.id)
    await message.reply_text(f"Successfully removed sudo privileges from {target_user.first_name}!")

@owner_only
async def remove_og_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Please reply to a user's message to remove OG status!")
        return
    target_user = message.reply_to_message.from_user
    db = get_database()
    await db.remove_og(target_user.id)
    await message.reply_text(f"Successfully removed OG status from {target_user.first_name}!")

@admin_only
async def view_admins_command(client: Client, message: Message):
    db = get_database()
    sudos = await db.users.find({'sudo': True}).to_list(length=None)
    ogs = await db.users.find({'og': True}).to_list(length=None)
    msg = "<b>Marvel Collector Bot Admins 👑</b>\n\n"
    msg += "<b>👑 Owner:</b>\n"
    owner_user = await db.get_user(OWNER_ID)
    if owner_user:
        owner_name = owner_user.get('first_name', 'Unknown')
        owner_id = owner_user.get('user_id', 'No ID')
        msg += f"• {owner_name} | ID: <code>{owner_id}</code>\n\n"
    msg += "<b>🌟 OG Admins:</b>\n"
    if ogs:
        for og in ogs:
            name = og.get('first_name', 'Unknown')
            user_id = og.get('user_id', 'No ID')
            msg += f"• {name} | ID: <code>{user_id}</code>\n"
    else:
        msg += "• No OG admins yet\n"
    msg += "\n<b>⭐ Sudo Admins:</b>\n"
    if sudos:
        for sudo in sudos:
            name = sudo.get('first_name', 'Unknown')
            user_id = sudo.get('user_id', 'No ID')
            msg += f"• {name} | ID: <code>{user_id}</code>\n"
    else:
        msg += "• No sudo admins yet\n"
    await message.reply_text(msg)

async def info_command(client: Client, message: Message):
    user_id = message.from_user.id
    db = get_database()
    if not (is_owner(user_id) or await is_og(db, user_id)):
        await message.reply_text(
            "<b>❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴏᴡɴᴇʀ ᴀɴᴅ ᴏɢs ᴏɴʟʏ!</b>"
        )
        return
    target_user_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        target_user_id = int(args[1])
    elif message.reply_to_message:
        target_user_id = message.reply_to_message.from_user.id
    if not target_user_id:
        await message.reply_text(
            "<b>❌ Please reply to a user's message or provide a user ID!\nUsage: /info <user_id></b>"
        )
        return
    await send_info_panel(client, message, target_user_id)

async def send_info_panel(client, message_or_callback, user_id):
    db = get_database()
    # Only fetch needed fields
    user_data = await db.users.find_one({"user_id": user_id}, {"user_id": 1, "first_name": 1, "wallet": 1, "bank": 1, "shards": 1, "characters": 1})
    if not user_data:
        if isinstance(message_or_callback, Message):
            await message_or_callback.reply_text("<b>❌ ᴜsᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ ɪɴ ᴅᴀᴛᴀʙᴀsᴇ!</b>")
        elif isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.answer("User not found!", show_alert=True)
        return
    
    try:
        # Use get_user_collection for details
        collection = await db.get_user_collection(user_id)
        unique_chars = len(collection)
        total_chars = sum(c['count'] for c in collection)
        rarity_counts = Counter()
        for char in collection:
            rarity_counts[char['rarity']] += char['count']
        
        # Sanitize user data to prevent HTML formatting issues
        first_name = str(user_data.get('first_name', 'Unknown')).replace('<', '&lt;').replace('>', '&gt;')
        user_id_safe = str(user_data['user_id'])
        wallet = user_data.get('wallet', 0)
        bank = user_data.get('bank', 0)
        shards = user_data.get('shards', 0)
        
        info = (
            f"<b>👤 ᴜsᴇʀ ɪɴғᴏʀᴍᴀᴛɪᴏɴ</b>\n\n"
            f"<b>ɴᴀᴍᴇ:</b> {first_name}\n"
            f"<b>ɪᴅ:</b> <code>{user_id_safe}</code>\n\n"
            f"<b>💰 ᴇᴄᴏɴᴏᴍʏ</b>\n"
            f"└ <b>ᴡᴀʟʟᴇᴛ:</b> <code>{wallet:,}</code>\n"
            f"└ <b>ʙᴀɴᴋ:</b> <code>{bank:,}</code>\n"
            f"└ <b>🎐 sʜᴀʀᴅs:</b> <code>{shards:,}</code>\n\n"
            f"<b>📊 ᴄᴏʟʟᴇᴄᴛɪᴏɴ sᴛᴀᴛs</b>\n"
            f"└ <b>ᴛᴏᴛᴀʟ:</b> <code>{total_chars}</code>\n"
            f"└ <b>ᴜɴɪǫᴜᴇ:</b> <code>{unique_chars}</code>\n\n"
            f"<b>🎭 ʀᴀʀɪᴛʏ ʙʀᴇᴀᴋᴅᴏᴡɴ</b>\n"
        )
        
        # Safely add rarity breakdown
        for rarity, count in sorted(rarity_counts.items()):
            try:
                emoji = get_rarity_emoji(rarity)
                # Sanitize rarity name
                safe_rarity = str(rarity).replace('<', '&lt;').replace('>', '&gt;')
                info += f"└ <b>{emoji} {safe_rarity}:</b> <code>{count}</code>\n"
            except Exception as e:
                # If there's an issue with a specific rarity, skip it
                print(f"Error processing rarity {rarity}: {e}")
                continue
        
        # Check if the user viewing this is an admin
        viewer_id = None
        if isinstance(message_or_callback, Message):
            viewer_id = message_or_callback.from_user.id
        elif isinstance(message_or_callback, CallbackQuery):
            viewer_id = message_or_callback.from_user.id
        
        is_admin = False
        if viewer_id:
            viewer_data = await db.get_user(viewer_id)
            if viewer_data:
                is_admin = is_owner(viewer_id) or viewer_data.get('sudo') or viewer_data.get('og')
        
        keyboard = []
        if is_admin:
            # Admin panel button
            keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data=f"info_admin_{user_id}")])
        else:
            # Close button for normal users
            keyboard.append([InlineKeyboardButton("❌ Close", callback_data=f"info_close_{user_id}")])
        
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(info, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await message_or_callback.reply_text(info, reply_markup=InlineKeyboardMarkup(keyboard))
            
    except Exception as e:
        # Fallback error message if something goes wrong
        error_msg = f"<b>❌ Error displaying user info: {str(e)}</b>"
        try:
            if isinstance(message_or_callback, CallbackQuery):
                await message_or_callback.message.edit_text(error_msg)
            else:
                await message_or_callback.reply_text(error_msg)
        except Exception as fallback_error:
            # If even the error message fails, try plain text
            try:
                if isinstance(message_or_callback, CallbackQuery):
                    await message_or_callback.answer("Error displaying user info", show_alert=True)
                else:
                    await message_or_callback.reply_text("❌ Error displaying user info")
            except:
                # Last resort - just log the error
                print(f"Failed to send error message: {fallback_error}")
                print(f"Original error: {e}")

async def info_callback(client: Client, callback_query: CallbackQuery):
    db = get_database()
    parts = callback_query.data.split("_", 3)
    if len(parts) == 3:
        _, action, user_id = parts
        confirm_action = None
    elif len(parts) == 4:
        _, action, confirm_action, user_id = parts
    else:
        await callback_query.answer("Invalid callback data!", show_alert=True)
        return
    try:
        user_id = int(user_id)
    except Exception:
        await callback_query.answer("Invalid user id!", show_alert=True)
        return

    # Check if user is admin
    viewer_id = callback_query.from_user.id
    viewer_data = await db.get_user(viewer_id)
    is_admin = False
    if viewer_data:
        is_admin = is_owner(viewer_id) or viewer_data.get('sudo') or viewer_data.get('og')

    if action == "admin":
        if not is_admin:
            await callback_query.answer("Nikal Bhosdike", show_alert=True)
            return
        # Show admin panel with reset options
        keyboard = [
            [
                InlineKeyboardButton("🗑️ Reset Collection", callback_data=f"info_collection_{user_id}"),
                InlineKeyboardButton("💰 Reset Wallet", callback_data=f"info_wallet_{user_id}")
            ],
            [
                InlineKeyboardButton("🎐 Reset Shards", callback_data=f"info_shards_{user_id}"),
                InlineKeyboardButton("⚠️ Full Reset", callback_data=f"info_full_{user_id}")
            ],
            [
                InlineKeyboardButton("❌ Close", callback_data=f"info_close_{user_id}")
            ]
        ]
        await callback_query.message.edit_reply_markup(InlineKeyboardMarkup(keyboard))
        return
    elif action in ("collection", "wallet", "shards", "full"):
        if not is_admin:
            await callback_query.answer("Nikal Bhosdike", show_alert=True)
            return
        # Show confirmation dialog
        action_map = {
            "collection": "reset this user's collection",
            "wallet": "reset this user's wallet",
            "shards": "reset this user's shards",
            "full": "fully reset this user's account"
        }
        confirm_text = f"⚠️ Are you sure you want to {action_map[action]}?"
        keyboard = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"info_confirm_{action}_{user_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"info_cancel_{user_id}")
            ]
        ]
        await callback_query.message.edit_text(confirm_text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    elif action == "confirm" and confirm_action:
        if not is_admin:
            await callback_query.answer("Nikal Bhosdike", show_alert=True)
            return
        # info_confirm_{action}_{user_id}
        if confirm_action == "collection":
            # Get user data before reset for logging
            user_data_before = await db.get_user(user_id)
            await db.update_user(user_id, {"characters": []})
            await callback_query.answer("Collection reset!")
            await send_info_panel(client, callback_query, user_id)
            # Log the reset action
            await log_reset_action(client, callback_query.from_user, user_id, "collection", user_data_before)
        elif confirm_action == "wallet":
            # Get user data before reset for logging
            user_data_before = await db.get_user(user_id)
            await db.update_user(user_id, {"wallet": 0})
            await callback_query.answer("Wallet reset!")
            await send_info_panel(client, callback_query, user_id)
            # Log the reset action
            await log_reset_action(client, callback_query.from_user, user_id, "wallet", user_data_before)
        elif confirm_action == "shards":
            # Get user data before reset for logging
            user_data_before = await db.get_user(user_id)
            await db.update_user(user_id, {"shards": 0})
            await callback_query.answer("Shards reset!")
            await send_info_panel(client, callback_query, user_id)
            # Log the reset action
            await log_reset_action(client, callback_query.from_user, user_id, "shards", user_data_before)
        elif confirm_action == "full":
            # Get user data before reset for logging
            user_data_before = await db.get_user(user_id)
            await db.update_user(user_id, {"characters": [], "wallet": 0, "bank": 0, "shards": 0})
            await callback_query.answer("Full reset done!")
            await send_info_panel(client, callback_query, user_id)
            # Log the reset action
            await log_reset_action(client, callback_query.from_user, user_id, "full", user_data_before)
        else:
            await callback_query.answer("Unknown action!", show_alert=True)
        return
    elif action == "cancel":
        # info_cancel_{user_id}
        await send_info_panel(client, callback_query, user_id)
        return
    elif action == "close":
        await callback_query.message.delete()
        return
    else:
        await callback_query.answer("Unknown action!", show_alert=True)
        return

@admin_only
async def reset_drop_weights_command(client: Client, message: Message):
    db = get_database()
    await setup_drop_weights_and_limits(db)
    await send_admin_log(client, message.from_user, "Reset drop weights and limits", target=None)
    await message.reply_text("✅ <b>Drop weights and limits have been reset!</b>")

@owner_only
async def donate_command(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Please reply to a user's message to donate characters!")
        return
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.reply_text("Usage: /donate <number> (reply to user)")
        return
    num = int(args[1])
    if num < 1 or num > 999999:
        await message.reply_text("You can donate between 1 and 999999 characters at once.")
        return
    target_user = message.reply_to_message.from_user
    db = get_database()
    # Fetch all characters once
    all_chars = await db.get_all_characters()
    if not all_chars:
        await message.reply_text("No characters available to donate!")
        return
    char_samples = random.choices(all_chars, k=num) if num > len(all_chars) else random.sample(all_chars, k=num)
    char_ids = [c['character_id'] for c in char_samples]
    # Single DB update
    await db.users.update_one({'user_id': target_user.id}, {'$push': {'characters': {'$each': char_ids}}})
    # Build summary (show only first 20 for preview)
    lines = [f"• <b>{c['name']}</b> (<code>{c['character_id']}</code>) - {c['rarity']}" for c in char_samples[:20]]
    msg = (
        f"<b>🎁 Donated {len(char_samples)} random characters to {target_user.mention}:</b>\n\n" + '\n'.join(lines)
    )
    if len(char_samples) > 20:
        msg += f"\n...and {len(char_samples) - 20} more."
    await message.reply_text(msg)

@owner_only
async def reset_users_command(client, message):
    db = get_database()
    # Only allow in private chat
    if message.chat.type != ChatType.PRIVATE:
        await message.reply_text("❌ This command can only be used in private chat.")
        return
    # Log the admin action
    await send_admin_log(client, message.from_user, "Initiated reset of ALL users", target=None, extra=f"Chat ID: {message.chat.id}")
    # Confirmation button
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Confirm Reset ALL Users", callback_data="resetusers_confirm")]
    ])
    await message.reply_text(
        "⚠️ <b>Are you sure you want to delete ALL users from the database?</b>\n\nThis cannot be undone!",
        reply_markup=keyboard
    )

@owner_only
async def reset_users_confirm_callback(client, callback_query):
    db = get_database()
    await db.users.delete_many({})
    # Log the admin action
    await send_admin_log(client, callback_query.from_user, "Confirmed reset of ALL users", target=None, extra=f"Chat ID: {callback_query.message.chat.id if callback_query.message else 'N/A'}")
    await callback_query.edit_message_text("✅ <b>All users have been deleted from the database.</b>")

@owner_only
async def backup_command(client: Client, message: Message):
    # MongoDB connection (adjust as needed)
    mongo_uri = "mongodb+srv://vegetakun447:Swami447@cluster0.hcngy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "marvel_collector"
    client_mongo = pymongo.MongoClient(mongo_uri)
    db = client_mongo[db_name]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"json_backup_{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)

    def json_default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    # Export each collection to JSON
    for collection_name in db.list_collection_names():
        data = list(db[collection_name].find())
        # Convert ObjectId to string for JSON serialization
        for doc in data:
            doc["_id"] = str(doc["_id"])
        with open(f"{backup_dir}/{collection_name}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=json_default)

    # Zip the backup
    zip_name = f"{backup_dir}.zip"
    shutil.make_archive(backup_dir, 'zip', backup_dir)

    # Send the zip file
    await message.reply_document(zip_name, caption="🗄️ JSON database backup completed!")

    # Clean up
    shutil.rmtree(backup_dir)
    os.remove(zip_name)
    client_mongo.close()

@owner_only
async def backup_shell_command(client: Client, message: Message):
    """Create a MongoDB backup using mongodump and send it to the owner."""
    import subprocess
    import shutil
    import os
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"mongodump_backup_{timestamp}"
    zip_name = f"{backup_dir}.zip"
    mongo_uri = "mongodb+srv://vegetakun447:Swami447@cluster0.hcngy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "marvel_collector"
    try:
        # Run mongodump with URI
        cmd = [
            "mongodump",
            f"--uri={mongo_uri}",
            f"--db={db_name}",
            f"--out={backup_dir}"
        ]
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            await message.reply_text(f"❌ Shell backup failed!\n{result.stderr.decode()}")
            return
        # Zip the backup folder
        shutil.make_archive(backup_dir, 'zip', backup_dir)
        # Send the zip file
        await message.reply_document(zip_name, caption="🗄️ Shell mongodump backup completed!")
    except Exception as e:
        await message.reply_text(f"❌ Shell backup failed! {e}")
    finally:
        # Clean up
        if os.path.exists(zip_name):
            os.remove(zip_name)
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)

@owner_only
async def backup_cmd(client: Client, message: Message):
    """Create a backup using shell script."""
    await message.reply_text("🔄 Backup started...")

    proc = await asyncio.create_subprocess_shell(
        "/bin/bash /root/mongo_backup.sh",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode == 0:
        await message.reply_text("✅ Backup completed!")
    else:
        await message.reply_text(f"❌ Backup failed:\n{stderr.decode()}")

@owner_only
async def track_command(client: Client, message: Message):
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.reply_text("Usage: /track <user_id>")
        return
    user_id = int(args[1])
    db = get_database()
    user = await db.get_user(user_id)
    if not user:
        await message.reply_text("User not found.")
        return
    history = user.get("transaction_history", [])[:25]
    if not history:
        await message.reply_text("No transactions found for this user.")
        return
    lines = [f"<b>Last 25 transactions for user <code>{user_id}</code>:</b>\n"]
    for entry in history:
        t = entry.get("type", "?")
        ts = entry.get("timestamp")
        details = entry.get("details", {})
        # Format timestamp
        if hasattr(ts, 'strftime'):
            ts_str = ts.strftime('%Y-%m-%d %H:%M')
        else:
            ts_str = str(ts)
        # Format details
        dstr = ', '.join(f"{k}: {v}" for k, v in details.items())
        lines.append(f"<b>{ts_str}</b> | <code>{t}</code> | {dstr}")
    await message.reply_text('\n'.join(lines))

async def log_reset_action(client, admin_user, target_user_id, reset_type, user_data_before):
    """Log reset actions to both log channels and create pastebin link"""
    try:
        from config import LOG_CHANNEL_ID, DROPTIME_LOG_CHANNEL
        from datetime import datetime
        import aiohttp
        
        # Get target user details
        target_user = await client.get_users(target_user_id)
        admin_name = admin_user.first_name if admin_user.first_name else "Unknown"
        target_name = target_user.first_name if target_user.first_name else "Unknown"
        
        # Format timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create pastebin content based on reset type
        if reset_type == "collection":
            pastebin_content = f"""Timestamp: {timestamp}
Admin ID: {admin_user.id}
Admin Name: {admin_name}
Target User ID: {target_user_id}
Target User Name: {target_name}
Collection Size: {len(user_data_before.get('characters', []))}
Character IDs: {', '.join(map(str, user_data_before.get('characters', [])))}"""
            
            action_type = "Collection Deleted"
            details = f"{len(user_data_before.get('characters', []))} items"
            
        elif reset_type == "wallet":
            pastebin_content = f"""Timestamp: {timestamp}
Admin ID: {admin_user.id}
Admin Name: {admin_name}
Target User ID: {target_user_id}
Target User Name: {target_name}
Wallet: {user_data_before.get('wallet', 0):,} tokens
Bank: {user_data_before.get('bank', 0):,} tokens"""
            
            action_type = "Wallet Reset"
            details = f"Wallet: {user_data_before.get('wallet', 0):,} | Bank: {user_data_before.get('bank', 0):,}"
            
        elif reset_type == "shards":
            pastebin_content = f"""Timestamp: {timestamp}
Admin ID: {admin_user.id}
Admin Name: {admin_name}
Target User ID: {target_user_id}
Target User Name: {target_name}
Shards: {user_data_before.get('shards', 0):,}
Wallet: {user_data_before.get('wallet', 0):,} tokens
Bank: {user_data_before.get('bank', 0):,} tokens"""
            
            action_type = "Shards Reset"
            details = f"Shards: {user_data_before.get('shards', 0):,} | Wallet: {user_data_before.get('wallet', 0):,} | Bank: {user_data_before.get('bank', 0):,}"
            
        else:  # full reset
            pastebin_content = f"""Timestamp: {timestamp}
Admin ID: {admin_user.id}
Admin Name: {admin_name}
Target User ID: {target_user_id}
Target User Name: {target_name}
Wallet: {user_data_before.get('wallet', 0):,} tokens
Bank: {user_data_before.get('bank', 0):,} tokens
Collection Size: {len(user_data_before.get('characters', []))}
Character IDs: {', '.join(map(str, user_data_before.get('characters', [])))}"""
            
            action_type = "Full Account Reset"
            details = f"Wallet: {user_data_before.get('wallet', 0):,} | Bank: {user_data_before.get('bank', 0):,} | {len(user_data_before.get('characters', []))} items"
        
        # Create pastebin link
        pastebin_url = await create_pastebin_link(pastebin_content, f"Admin Reset - {action_type}")
        
        # Format log message
        log_message = (
            f"📜 <b>Admin Action Log</b>\n"
            f"🛠 <b>Action:</b> {action_type}\n"
            f"👮 <b>Admin:</b> {admin_name} ({admin_user.id})\n"
            f"🎯 <b>Target:</b> {target_name} ({target_user_id})\n"
            f"📝 <b>Details:</b> {details} - <a href='{pastebin_url}'>View Backup</a>\n"
            f"🕒 <b>Timestamp:</b> {timestamp}"
        )
        
        # Send to both log channels
        try:
            # Send to main log channel
            await client.send_message(
                chat_id=LOG_CHANNEL_ID,
                text=log_message,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"Error sending to main log channel: {e}")
        
        try:
            # Send to droptime log channel
            await client.send_message(
                chat_id=DROPTIME_LOG_CHANNEL,
                text=log_message,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"Error sending to droptime log channel: {e}")
            
    except Exception as e:
        print(f"Error in log_reset_action: {e}")

async def create_pastebin_link(content, title):
    """Create a pastebin link for large content"""
    try:
        import requests
        response = requests.post(
            'https://pastebin.com/api/api_post.php',
            data={
                'api_dev_key': 'your_pastebin_api_key_here',  # You'll need to get this
            'api_option': 'paste',
            'api_paste_code': content,
                'api_paste_name': title
            }
        )
        if response.status_code == 200:
            return response.text
        else:
            return None
    except Exception as e:
        print(f"Error creating pastebin link: {e}")
        return None

# Database monitoring commands
