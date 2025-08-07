from pyrogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InlineQuery, InlineQueryResultPhoto, InlineQueryResultVideo, InlineQueryResultArticle, InputTextMessageContent, InputMediaPhoto, InputMediaVideo
from pyrogram.errors import MessageNotModified
import os

# Import database based on configuration
from modules.postgres_database import get_database
import random
import re
from collections import Counter
import asyncio
from datetime import datetime

ITEMS_PER_PAGE = 10

# Separate rarity definitions
RARITY_DATA = {
    "Common": {
        "emoji": "⚪️",
        "level": 1
    },
    "Medium": {
        "emoji": "🟢",
        "level": 2
    },
    "Rare": {
        "emoji": "🟠",
        "level": 3
    },
    "Legendary": {
        "emoji": "🟡",
        "level": 4
    },
    "Exclusive": {
        "emoji": "🫧",
        "level": 5
    },
    "Elite": {
        "emoji": "💎",
        "level": 6
    },
    "Limited Edition": {
        "emoji": "🔮",
        "level": 7
    },
    "Ultimate": {
        "emoji": "🔱",
        "level": 8
    },
    "Supreme": {
        "emoji": "👑",
        "level": 9
    },
    "Zenith": {
        "emoji": "💫",
        "level": 10
    },
    "Ethereal": {
        "emoji": "❄️",
        "level": 11
    },
    "Mythic": {
        "emoji": "🔴",
        "level": 12
    },
    "Premium": {
        "emoji": "🧿",
        "level": 13
    }
}

# Helper functions for rarity handling
def get_rarity_parts(rarity_full: str) -> tuple:
    """Split full rarity string into emoji and name"""
    for rarity_name, data in RARITY_DATA.items():
        if rarity_name in rarity_full:
            return data["emoji"], rarity_name
    return "⭐", rarity_full  # Default fallback

def get_rarity_level(rarity_full: str) -> int:
    """Get rarity level for sorting"""
    for rarity_name, data in RARITY_DATA.items():
        if rarity_name in rarity_full:
            return data["level"]
    return 0  # Default fallback

async def collection_command(client, message: Message):
    """Handle /mycollection command (Pyrogram version)"""
    try:
        user_id = message.from_user.id
        db = get_database()

        # Get user data using PostgreSQL-compatible method
        if hasattr(db, 'pool'):  # PostgreSQL
            user_data = await db.get_user(user_id)
        else:  # MongoDB
            user_data = await db.users.find_one({"user_id": user_id}, {"user_id": 1, "first_name": 1, "favorite_character": 1})
        
        if not user_data:
            # Try to create user if they don't exist
            try:
                user_data = {
                    'user_id': user_id,
                    'username': message.from_user.username,
                    'first_name': message.from_user.first_name,
                    'last_name': message.from_user.last_name,
                    'wallet': 0,
                    'shards': 0,
                    'characters': [],
                    'coins': 100,
                    'last_daily': None,
                    'last_weekly': None,
                    'last_monthly': None,
                    'sudo': False,
                    'og': False,
                    'collection_preferences': {
                        'mode': 'default',
                        'filter': None
                    },
                    'joined_at': datetime.now()
                }
                await db.add_user(user_data)
            except Exception as e:
                print(f"Error creating user: {e}")
                await message.reply_text(
                    "<b>❌ ᴘʟᴇᴀsᴇ sᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ ғɪʀsᴛ ʙʏ ᴜsɪɴɢ /start ᴄᴏᴍᴍᴀɴᴅ!</b>"
                )
                return

        # Ensure user_data is a dictionary
        if not isinstance(user_data, dict):
            await message.reply_text(
                "<b>❌ ᴇʀʀᴏʀ ʀᴇᴀᴅɪɴɢ ᴜsᴇʀ ᴅᴀᴛᴀ!</b>"
            )
            return

        # Get preferences directly from database
        preferences = await db.get_user_preferences(user_id)
        mode = preferences.get('mode', 'default')
        rarity_filter = preferences.get('filter', None)

        # Get unique character count using optimized method
        collection = await db.get_user_collection(user_id)
        unique_count = len(collection)

        # Create keyboard with inline query button
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🔍 View Collection ({unique_count} unique)", switch_inline_query_current_chat=f"collection:{user_id}:0")]
        ])

        # Show collection page with current preferences
        await show_collection_page(
            client,
            message,
            user_id,
            page=1,
            mode=mode,
            rarity_filter=rarity_filter,
            reply_markup=keyboard,
            from_user=message.from_user
        )
    except Exception as e:
        print(f"Error in collection_command: {e}")
        await message.reply_text(
            "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ!</b>"
        )

async def batch_fetch_characters(db, char_ids, batch_size=500):
    """Fetch characters in batches to avoid memory issues"""
    if not char_ids:
        return []
    
    projection = {
        'character_id': 1,
        'name': 1,
        'rarity': 1,
        'img_url': 1,
        'file_id': 1,
        'is_video': 1
    }
    
    if hasattr(db, 'pool'):  # PostgreSQL
        async with db.pool.acquire() as conn:
            # Convert list to tuple for SQL IN clause
            char_ids_tuple = tuple(char_ids)
            if len(char_ids_tuple) == 1:
                # Handle single item case
                char_ids_tuple = (char_ids_tuple[0],)
            
            characters = await conn.fetch(
                "SELECT character_id, name, rarity, img_url, file_id, is_video FROM characters WHERE character_id = ANY($1)",
                char_ids_tuple
            )
            return [dict(char) for char in characters]
    else:  # MongoDB
        batches = [char_ids[i:i+batch_size] for i in range(0, len(char_ids), batch_size)]
        async def fetch_batch(batch):
            return await db.characters.find({'character_id': {'$in': batch}}, projection).to_list(length=None)
        results = await asyncio.gather(*(fetch_batch(batch) for batch in batches))
        char_docs = [doc for batch in results for doc in batch]
        return char_docs

async def show_collection_page(client, message, user_id: int, page: int, mode='default', rarity_filter=None, reply_markup=None, from_user=None, callback_query=None):
    """Show a page of the user's collection (Pyrogram version)"""
    try:
        db = get_database()
        # Get user data using PostgreSQL-compatible method
        if hasattr(db, 'pool'):  # PostgreSQL
            user_data = await db.get_user(user_id)
        else:  # MongoDB
            user_data = await db.users.find_one({"user_id": user_id}, {"user_id": 1, "first_name": 1, "favorite_character": 1})
        
        collection = await db.get_user_collection(user_id)
        total_items = len(collection)
        if rarity_filter:
            collection = [c for c in collection if c['rarity'] == rarity_filter]
        total_pages = max(1, (len(collection) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(collection))
        current_page_items = collection[start_idx:end_idx]
        display_name = from_user.first_name if from_user else user_data.get('first_name', 'User')
        text = _create_collection_message(
            display_name,
            total_items,
            page,
            total_pages,
            current_page_items,
            mode=mode,
            rarity_filter=rarity_filter
        )
        keyboard = _create_keyboard(page, total_pages, user_id, total_items)
        reply_markup = keyboard
        # Only fetch favorite_character if needed
        favorite_id = None
        if user_data and 'favorite_character' in user_data:
            favorite_id = user_data.get('favorite_character')
        favorite_char = None
        if favorite_id:
            # Use batch fetch for favorite character
            if hasattr(db, 'pool'):  # PostgreSQL
                async with db.pool.acquire() as conn:
                    char_docs = await conn.fetch(
                        "SELECT character_id, name, rarity, file_id, img_url, is_video FROM characters WHERE character_id = $1",
                        favorite_id
                    )
                    char_docs = [dict(char) for char in char_docs]
            else:  # MongoDB
                char_docs = await db.characters.find({"character_id": favorite_id}, {"character_id": 1, "name": 1, "rarity": 1, "file_id": 1, "img_url": 1, "is_video": 1}).to_list(length=1)
            
            if char_docs:
                # Check if user actually owns this character (use full collection, not filtered)
                full_collection = await db.get_user_collection(user_id)
                if favorite_id in [char['character_id'] for char in full_collection]:
                    favorite_char = char_docs[0]
                else:
                    # User doesn't own this character anymore, clear favorite
                    await db.update_user(user_id, {'favorite_character': None})
                    favorite_id = None
                    favorite_char = None
            else:
                # Character doesn't exist in database, clear favorite
                await db.update_user(user_id, {'favorite_character': None})
                favorite_id = None
                favorite_char = None
        if not favorite_id and collection:
            # If no favorite is set, set a random character as favorite and save it
            # Use the full collection (before rarity filter) for random selection
            full_collection = await db.get_user_collection(user_id)
            if full_collection:
                random_char = random.choice(full_collection)
                await db.update_user(user_id, {'favorite_character': random_char['character_id']})
                # Get the full character data for display
                if hasattr(db, 'pool'):  # PostgreSQL
                    async with db.pool.acquire() as conn:
                        char_docs = await conn.fetch(
                            "SELECT character_id, name, rarity, file_id, img_url, is_video FROM characters WHERE character_id = $1",
                            random_char['character_id']
                        )
                        char_docs = [dict(char) for char in char_docs]
                else:  # MongoDB
                    char_docs = await db.characters.find({"character_id": random_char['character_id']}, {"character_id": 1, "name": 1, "rarity": 1, "file_id": 1, "img_url": 1, "is_video": 1}).to_list(length=1)
                
                if char_docs:
                    favorite_char = char_docs[0]
        elif not favorite_id:
            favorite_char = None
        if favorite_char:
            is_video = favorite_char.get('is_video', False)
            if is_video:
                # For video characters, prefer img_url (Cloudinary URL) over file_id
                video_source = favorite_char.get('img_url') or favorite_char.get('file_id')
                if callback_query:
                    try:
                        if reply_markup:
                            await callback_query.edit_message_media(
                                media=InputMediaVideo(
                                    media=video_source,
                                    caption=text
                                ),
                                reply_markup=reply_markup
                            )
                        else:
                            await callback_query.edit_message_media(
                                media=InputMediaVideo(
                                    media=video_source,
                                    caption=text
                                )
                            )
                    except MessageNotModified:
                        pass
                    except Exception as e:
                        print(f"Error editing video message: {e}")
                        # Fallback to text message
                        await callback_query.edit_message_text(
                            text,
                            reply_markup=reply_markup
                        )
                else:
                    try:
                        await message.reply_video(
                            video=video_source,
                            caption=text,
                            reply_markup=reply_markup
                        )
                    except Exception as e:
                        print(f"Error sending video message: {e}")
                        # Fallback to text message
                        await message.reply_text(
                            text,
                            reply_markup=reply_markup
                        )
            else:
                photo = favorite_char.get('img_url', favorite_char['file_id'])
                if callback_query:
                    try:
                        if reply_markup:
                            await callback_query.edit_message_media(
                                media=InputMediaPhoto(
                                    media=photo,
                                    caption=text
                                ),
                                reply_markup=reply_markup
                            )
                        else:
                            await callback_query.edit_message_media(
                                media=InputMediaPhoto(
                                    media=photo,
                                    caption=text
                                )
                            )
                    except MessageNotModified:
                        pass
                    except Exception as e:
                        print(f"Error editing photo message: {e}")
                        # Fallback to text message
                        await callback_query.edit_message_text(
                            text,
                            reply_markup=reply_markup
                        )
                else:
                    try:
                        await message.reply_photo(
                            photo=photo,
                            caption=text,
                            reply_markup=reply_markup
                        )
                    except Exception as e:
                        print(f"Error sending photo message: {e}")
                        # Fallback to text message
                        await message.reply_text(
                            text,
                            reply_markup=reply_markup
                        )
        else:
            if callback_query:
                try:
                    if reply_markup:
                        await callback_query.edit_message_text(
                            text,
                            reply_markup=reply_markup
                        )
                    else:
                        await callback_query.edit_message_text(
                            text
                        )
                except MessageNotModified:
                    pass
            else:
                await message.reply_text(
                    text,
                    reply_markup=reply_markup
                )
    except Exception as e:
        print(f"Error in show_collection_page: {e}")
        error_message = "<b>❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ sʜᴏᴡɪɴɢ ʏᴏᴜʀ ᴄᴏʟʟᴇᴄᴛɪᴏɴ!</b>"
        if callback_query:
            await callback_query.edit_message_text(
                error_message
            )
        else:
            await message.reply_text(
                error_message
            )

async def smode_command(client, message):
    keyboard = [
        [InlineKeyboardButton("Sort by Rarity 📊", callback_data="sm_rarity"), InlineKeyboardButton("Default Mode 📱", callback_data="sm_default")],
        [InlineKeyboardButton("Detailed Mode 📋", callback_data="sm_detailed")],
        [InlineKeyboardButton("Close ❌", callback_data="sm_close")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message.reply_text(
        "<b>Display Settings</b>\nChoose how you want to view your collection:",
        reply_markup=reply_markup
    )

async def handle_smode_callback(client, callback_query: CallbackQuery):
    query = callback_query
    await query.answer()

    user_id = query.from_user.id
    db = get_database()
    current_preferences = await db.get_user_preferences(user_id)

    if query.data == "sm_close":
        await query.message.delete()
        return

    elif query.data == "sm_rarity":
        # Show rarity selection buttons
        keyboard = []
        for rarity_name, data in RARITY_DATA.items():
            keyboard.append([InlineKeyboardButton(
                f"{data['emoji']} {rarity_name}",
                callback_data=f"f_{rarity_name}"
            )])
        keyboard.append([InlineKeyboardButton("Back 🔙", callback_data="sm_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "<b>Select Rarity Filter:</b>",
            reply_markup=reply_markup
        )
        return

    elif query.data.startswith("f_"):
        rarity = query.data.split("_", 1)[1]
        # Keep the current mode when changing rarity filter
        preferences = {
            "mode": current_preferences.get('mode', 'default'),
            "filter": rarity
        }
        await db.update_user_preferences(user_id, preferences)

        await query.edit_message_text(
            f"🔄 Yᴏᴜʀ ᴄᴏʟʟᴇᴄᴛɪᴏɴ sᴏʀᴛ sʏsᴛᴇᴍ ʜᴀs ʙᴇᴇɴ sᴜᴄᴄᴇssғᴜʟʟʏ sᴇᴛ ᴛᴏ: <b>{rarity}</b>"
        )
        return

    elif query.data == "sm_back":
        keyboard = [
            [InlineKeyboardButton("Sort by Rarity 📊", callback_data="sm_rarity"), InlineKeyboardButton("Default Mode 📱", callback_data="sm_default")],
            [InlineKeyboardButton("Detailed Mode 📋", callback_data="sm_detailed")],
            [InlineKeyboardButton("Close ❌", callback_data="sm_close")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "<b>Display Settings</b>\nChoose how you want to view your collection:",
            reply_markup=reply_markup
        )
        return

    elif query.data.startswith("sm_"):
        mode = query.data.split("_")[1]
        if mode == "detailed":
            preferences = {
                "mode": mode,
                "filter": current_preferences.get('filter', None)
            }
        elif mode == "default":
            preferences = {
                "mode": "default",
                "filter": None
            }
        await db.update_user_preferences(user_id, preferences)
        mode_name = "Dᴇᴛᴀɪʟᴇᴅ Mᴏᴅᴇ" if mode == "detailed" else "Dᴇғᴀᴜʟᴛ Mᴏᴅᴇ"
        await query.edit_message_text(
            f"🔄 Yᴏᴜʀ ᴄᴏʟʟᴇᴄᴛɪᴏɴ ɪɴᴛᴇʀғᴀᴄᴇ ʜᴀs ʙᴇᴇɴ sᴇᴛ ᴛᴏ: <b>{mode_name}</b>"
        )

async def handle_collection_callback(client, callback_query: CallbackQuery):
    query = callback_query
    await query.answer()
    
    if query.data == "current_page":
        return
    
    if query.data.startswith("c_"):
        # Parse callback data: c_<page>_<owner_user_id>
        parts = query.data.split("_")
        if len(parts) >= 3:
            page = int(parts[1])
            owner_user_id = int(parts[2])
            
            # Check if the user clicking is the collection owner
            if query.from_user.id != owner_user_id:
                await query.answer("❌ You can only navigate your own collection!", show_alert=True)
                return
            
            # Get current preferences to preserve mode and filter
            db = get_database()
            preferences = await db.get_user_preferences(query.from_user.id)
            mode = preferences.get('mode', 'default')
            rarity_filter = preferences.get('filter', None)
            await show_collection_page(
                client,
                query.message,
                query.from_user.id,
                page,
                mode=mode,
                rarity_filter=rarity_filter,
                from_user=query.from_user,
                callback_query=query
            )
        else:
            # Handle old format for backward compatibility
            page = int(query.data.split("_")[1])
            db = get_database()
            preferences = await db.get_user_preferences(query.from_user.id)
            mode = preferences.get('mode', 'default')
            rarity_filter = preferences.get('filter', None)
            await show_collection_page(
                client,
                query.message,
                query.from_user.id,
                page,
                mode=mode,
                rarity_filter=rarity_filter,
                from_user=query.from_user,
                callback_query=query
            )
    
    elif query.data.startswith("s_"):
        sort_type = query.data.split("_")[1]
        await sort_collection(client, query.message, query.from_user.id, sort_type, callback_query=query)

async def sort_collection(client, message, user_id: int, sort_type: str, callback_query=None):
    db = get_database()
    user_data = await db.get_user(user_id)

    if not user_data or not user_data.get('characters', []):
        return

    char_ids = user_data['characters']
    characters = []

    for char_id in char_ids:
        char = await db.get_character(char_id)
        if char:
            characters.append(char)

    if sort_type == "name":
        characters.sort(key=lambda x: x['name'].lower())
    elif sort_type == "rarity":
        characters.sort(key=lambda x: get_rarity_level(x['rarity']), reverse=True)

    # Update user's character order
    new_char_ids = [char['character_id'] for char in characters]
    await db.update_user(user_id, {'characters': new_char_ids})

    # Show first page of sorted collection
    await show_collection_page(
        client,
        message,
        user_id,
        page=1,
        from_user=message.from_user,
        callback_query=callback_query
    )

def _create_collection_message(first_name, total_chars, page, total_pages, characters, mode='default', rarity_filter=None):
    """Create the collection display message (HTML version for Pyrogram)"""
    try:
        # Base message with user info and pagination
        message = (
            f"<b>{first_name}'s {rarity_filter if rarity_filter else 'Collection'} Page {page} of {total_pages}</b>\n\n"
        )

        if not characters:
            return message  # Return just the header with total count if no characters

        if mode == "detailed":
            # Group by rarity first
            rarity_groups = {}
            for char in characters:
                rarity = char['rarity']
                if rarity not in rarity_groups:
                    rarity_groups[rarity] = []
                rarity_groups[rarity].append(char)

            # Display by rarity groups
            for rarity, chars in sorted(rarity_groups.items(), key=lambda x: get_rarity_level(x[0]), reverse=True):
                rarity_emoji = RARITY_DATA.get(rarity, {}).get('emoji', '⭐')
                message += f"<b>{rarity_emoji} {rarity}</b>\n"
                for char in chars:
                    count_display = f" [x{char['count']}]" if char['count'] > 1 else ""
                    message += (
                        f"({char['character_id']}) {char['name']}{count_display}\n"
                    )
                message += "\n"
        else:
            # Default mode - simple list with requested format
            for char in characters:
                rarity_emoji = RARITY_DATA.get(char['rarity'], {}).get('emoji', '⭐')
                count_display = f" [x{char['count']}]" if char['count'] > 1 else ""
                message += (
                    f"{char['name']}{count_display}\n"
                    f"{rarity_emoji} | {char['rarity']}\n"
                    f"ID: {char['character_id']}\n\n"
                )

        return message
    except Exception as e:
        print(f"Error creating collection message: {e}")
        return "<b>❌ Error displaying collection!</b>"

def _create_keyboard(page, total_pages, user_id, total_items):
    keyboard = []
    if total_pages > 1:
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️ ᴘʀᴇᴠɪᴏᴜs", callback_data=f"c_{page-1}_{user_id}"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("ɴᴇxᴛ ➡️", callback_data=f"c_{page+1}_{user_id}"))
        keyboard.append(nav_row)
    # Always add the View Collection button (no :0)
    keyboard.append([InlineKeyboardButton(f"🔍 View Collection ({total_items})", switch_inline_query_current_chat=f"collection:{user_id}")])
    return InlineKeyboardMarkup(keyboard) if keyboard else None

# Helper for inline results (adapted from search.py)
def create_inline_result(character, user_name):
    rarity_emoji = RARITY_DATA.get(character['rarity'], {}).get('emoji', '⭐')
    caption = (
        f"<b>{user_name}'s {character['rarity']} Collect</b>\n\n"
        f"👤<b>Name</b>: {character['name']}"
    )
    if character['count'] > 1:
        caption += f" [x{character['count']}]"
    caption += f"\n{rarity_emoji}<b>Rarity:</b> {character['rarity']}\n\n"
    caption += f"🔖<b>ID</b>: {character['character_id']}"
    title = f"{rarity_emoji} {character['name']}"
    description = f"🆔 {character['character_id']} | {character['rarity']}"
    if character.get('img_url'):
        if character.get('is_video', False):
            return InlineQueryResultVideo(
                id=str(character['character_id']),
                video_url=character['img_url'],
                thumb_url=character['img_url'],
                mime_type='video/mp4',
                title=title,
                description=description,
                caption=caption
            )
        else:
            return InlineQueryResultPhoto(
                id=str(character['character_id']),
                photo_url=character['img_url'],
                thumb_url=character['img_url'],
                title=title,
                description=description,
                caption=caption
            )
    else:
        return InlineQueryResultArticle(
            id=str(character['character_id']),
            title=title,
            description=description,
            input_message_content=InputTextMessageContent(
                message_text=caption
            )
        )

async def handle_inline_query(client, inline_query: InlineQuery):
    import re
    query = inline_query.query.strip()
    m = re.match(r'^collection:(\d+)[ :]*(.*)$', query)
    if not m:
        await inline_query.answer([], cache_time=1)
        return

    user_id = int(m.group(1))
    search_str = m.group(2).strip() if m.group(2) else ''
    try:
        offset = int(inline_query.offset) if inline_query.offset else 0
    except ValueError:
        offset = 0

    db = get_database()
    user_data = await db.get_user(user_id)
    if not user_data or not user_data.get('characters'):
        await inline_query.answer([
            InlineQueryResultArticle(
                id="no_results",
                title="No results found",
                description="You have no characters.",
                input_message_content=InputTextMessageContent(
                    message_text="No results found."
                )
            )
        ], cache_time=1)
        return

    # Deduplicate and count characters
    char_ids = user_data['characters']
    char_counts = Counter(char_ids)
    # Batch fetch all character details
    char_docs = await batch_fetch_characters(db, list(char_counts.keys()), batch_size=500)
    id_to_char = {c['character_id']: c for c in char_docs}
    collection = []
    for char_id, count in char_counts.items():
        char = id_to_char.get(char_id)
        if char:
            char = dict(char)  # copy
            char['count'] = count
            collection.append(char)

    user_name = user_data.get('first_name', 'User')
    # Filter by search string
    if search_str:
        s = search_str.lower()
        filtered = [c for c in collection if s in c['name'].lower() or s in c['rarity'].lower()]
    else:
        filtered = collection

    # Pagination
    items_per_page = 50
    start_idx = offset
    end_idx = min(start_idx + items_per_page, len(filtered))
    results = []
    for char in filtered[start_idx:end_idx]:
        rarity_emoji = RARITY_DATA.get(char['rarity'], {}).get('emoji', '⭐')
        count_display = f" [x{char['count']}]" if char.get('count', 1) > 1 else ""
        caption = (
            f"<b>{user_name}'s {char['rarity']} Collect</b>\n\n"
            f"👤<b>Name</b>: {char['name']}{count_display}\n"
            f"{rarity_emoji}<b>Rarity</b>:  {char['rarity']} \n\n"
            f"🔖<b>ID</b>: {char['character_id']}"
        )
        title = f"{char['name']} ({char['rarity']})"
        description = f"ID: {char['character_id']}"
        if char.get('img_url'):
            if char.get('is_video', False):
                results.append(
                    InlineQueryResultVideo(
                        id=str(char['character_id']),
                        video_url=char['img_url'],
                        thumb_url=char['img_url'],
                        mime_type='video/mp4',
                        title=title,
                        description=description,
                        caption=caption
                    )
                )
            else:
                results.append(
                    InlineQueryResultPhoto(
                        id=str(char['character_id']),
                        photo_url=char['img_url'],
                        thumb_url=char['img_url'],
                        title=title,
                        description=description,
                        caption=caption
                    )
                )
        else:
            results.append(
                InlineQueryResultArticle(
                    id=str(char['character_id']),
                    title=title,
                    description=description,
                    input_message_content=InputTextMessageContent(
                        message_text=caption
                    )
                )
            )

    next_offset = str(end_idx) if end_idx < len(filtered) else ""
    if not results:
        results.append(InlineQueryResultArticle(
            id="no_results",
            title="No results found",
            description="Try searching by name or rarity.",
            input_message_content=InputTextMessageContent(
                message_text="No results found."
            )
        ))
    await inline_query.answer(results, cache_time=1, next_offset=next_offset)
