import asyncio
import base64
import io
import logging
import mimetypes
import os

import aiohttp
import cloudinary
import cloudinary.uploader
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from .decorators import admin_only, check_banned, is_owner
from .decorators import is_og, is_sudo
from .postgres_database import (
    RARITIES,
    RARITY_EMOJIS,
    get_database,
    get_rarity_display,
    get_rarity_emoji,
)

logging.basicConfig(level=logging.INFO)

# Helper for markdown v2 escaping (legacy, for migration)
def escape_markdown(text, version=2):
    """Escape markdown characters"""
    return str(text).replace('_', r'\_').replace('*', r'\*').replace('[', r'\[').replace('`', r'\`')

# States
NAME, RARITY, IMAGE, CONFIRM = range(4)
EDIT_ID, EDIT_CHOICE, EDIT_NAME, EDIT_RARITY, EDIT_IMAGE = range(5, 10)
DELETE_ID, DELETE_CONFIRM = range(10, 12)
RESET_ID, RESET_CONFIRM = range(12, 14)  # New states for reset

# Channel ID for updates
UPDATE_CHANNEL = -1002527070449

# Rarity data with emojis
RARITIES = {
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
    "Mythic":{
        "emoji": "🔴",
        "level": 10
    },
    "Zenith":{
        "emoji": "💫",
        "level": 11
    },
    "Ethereal":{
        "emoji": "❄️",
        "level": 12
    },
    "Premium":{
        "emoji": "🧿",
        "level": 13
    }
}

# Module-level temp_data for state management
temp_data = {}

ADMIN_PANEL_STATE = {}

# Temporary state for pending add confirmations
PENDING_ADD = {}

PENDING_EDIT = {}

DROPTIME_LOG_CHANNEL = -1002558794123

@admin_only
async def add_character_command(client: Client, message: Message):
    user_id = message.from_user.id
    # Must be a reply to a photo or video
    if not message.reply_to_message or (not message.reply_to_message.photo and not message.reply_to_message.video):
        await message.reply_text("<b>❌ Please reply to an image or video with /add &lt;name&gt; &lt;rarity&gt;.</b>", parse_mode=ParseMode.HTML)
        return
    # Parse arguments
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.reply_text("<b>❌ Usage: /add &lt;name&gt; &lt;rarity&gt; (as a reply to an image or video)</b>", parse_mode=ParseMode.HTML)
        return
    # Improved multi-word name and rarity parsing
    input_text = args[1] + " " + args[2]
    input_text = input_text.strip()
    matched_rarity = None
    matched_name = None
    rarity_keys = sorted(RARITIES.keys(), key=lambda x: -len(x))  # Longest first
    for r in rarity_keys:
        if input_text.lower().endswith(r.lower()):
            matched_rarity = r
            matched_name = input_text[:-(len(r))].strip()
            break
    if not matched_rarity or not matched_name or len(matched_name) < 2 or len(matched_name) > 50:
        await message.reply_text(f"<b>❌ Usage: /add &lt;name&gt; &lt;rarity&gt; (as a reply to an image or video)\nValid rarities: {' | '.join(RARITIES.keys())}</b>", parse_mode=ParseMode.HTML)
        return
    name = matched_name
    
    # Determine if it's a photo or video and get file_id
    is_video = message.reply_to_message.video is not None
    if is_video:
        file_id = message.reply_to_message.video.file_id
        media_type = "video"
    else:
        file_id = message.reply_to_message.photo.file_id
        media_type = "image"
    
    # Show processing message
    processing_msg = await message.reply_text(f"<i>Processing your {media_type} upload, please wait...</i>", parse_mode=ParseMode.HTML)
    
    # Upload based on media type
    if is_video:
        # Use Cloudinary for videos (same logic as vid.py)
        try:
            file = await asyncio.wait_for(client.download_media(file_id), timeout=120)
            
            # Configure Cloudinary (same as vid.py)
            cloudinary.config(
                cloud_name="de96qtqav",
                api_key="755161292211756",
                api_secret="vO_1lOfhJQs3kI4C5v1E8fywYW8"
            )
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: cloudinary.uploader.upload(file, resource_type="video")
            )
            img_url = result["secure_url"]
        except Exception as e:
            await processing_msg.edit_text(f"<b>❌ Failed to upload video to Cloudinary. {e}</b>")
            return
    else:
        # Use ImgBB for images
        img_url = await upload_to_imgbb(file_id, client)
        if not img_url:
            await processing_msg.edit_text(f"<b>❌ Failed to upload image to ImgBB.</b>\n\n{get_upload_error_message()}", parse_mode=ParseMode.HTML)
            return
    
    db = get_database()
    # Store pending add with media type info
    PENDING_ADD[user_id] = {
        "name": name,
        "rarity": matched_rarity,
        "file_id": file_id,
        "img_url": img_url,
        "added_by": user_id,
        "is_video": is_video,
        "media_type": media_type
    }
    
    rarity_emoji = RARITIES[matched_rarity]["emoji"]
    keyboard = [
        [InlineKeyboardButton("✅ Confirm", callback_data="addchar_confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="addchar_cancel")],
    ]
    
    await processing_msg.delete()
    
    # Send preview based on media type
    if is_video:
        await message.reply_video(
            video=file_id,
            caption=(
                f"<b>🆕 Add Video Character Preview</b>\n\n"
                f"<b>👤 Name:</b> {name}\n"
                f"<b>✨ Rarity:</b> {rarity_emoji} {matched_rarity}\n"
                f"<b>📹 Type:</b> Video Character\n\n"
                f"Do you want to add this character?"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await message.reply_photo(
            photo=file_id,
            caption=(
                f"<b>🆕 Add Character Preview</b>\n\n"
                f"<b>👤 Name:</b> {name}\n"
                f"<b>✨ Rarity:</b> {rarity_emoji} {matched_rarity}\n\n"
                f"Do you want to add this character?"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# Confirm/Cancel callback handler
async def add_character_confirm_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = PENDING_ADD.get(user_id)
    if not data:
        await callback_query.answer("No pending character to confirm.", show_alert=True)
        return
    db = get_database()
    # Actually add to DB
    inserted_id = await db.add_character(data)
    char = await db.get_character(inserted_id)
    if char is None:
        char_id = inserted_id
        char_name = data['name']
        char_rarity = data['rarity']
        char_img_url = data.get('img_url', data.get('file_id'))
    else:
        char_id = char.get("character_id", inserted_id)
        char_name = char.get("name", data['name'])
        char_rarity = char.get("rarity", data['rarity'])
        char_img_url = char.get('img_url', data.get('file_id'))
    rarity_emoji = RARITIES[char_rarity]["emoji"]
    is_video = data.get("is_video", False)
    media_type_text = "Video Character" if is_video else "Character"
    await callback_query.message.edit_caption(
        f"<b>✅ {media_type_text} added!</b>\n\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>👤 Name:</b> {char_name}\n"
        f"<b>✨ Rarity:</b> {rarity_emoji} {char_rarity}\n"
        f"<b>📹 Type:</b> {media_type_text}\n"
    )
    PENDING_ADD.pop(user_id, None)
    admin_user = await client.get_users(user_id)
    log_caption = (
        f"<b>🆕 New {media_type_text.lower()} added</b>\n"
        f"<b>👤 Name:</b> {char_name}\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>✨ Rarity:</b> {rarity_emoji} {char_rarity}\n"
        f"<b>📹 Type:</b> {media_type_text}\n"
        f"<b>👮‍♂️ Added by:</b> {admin_user.mention if hasattr(admin_user, 'mention') else admin_user.first_name} (<code>{admin_user.id}</code>)"
    )
    
    # Send to log channel based on media type
    try:
        if is_video:
            await client.send_video(
                chat_id=DROPTIME_LOG_CHANNEL,
                video=char_img_url,
                caption=log_caption
            )
        else:
            await client.send_photo(
                chat_id=DROPTIME_LOG_CHANNEL,
                photo=char_img_url,
                caption=log_caption
            )
    except Exception as e:
        print(f"Failed to send character upload log to droptime channel: {e}")
        # Don't crash the bot, just log the error

async def add_character_cancel_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id in PENDING_ADD:
        PENDING_ADD.pop(user_id)
    await callback_query.message.edit_caption("<b>❌ Character addition cancelled.</b>", parse_mode=ParseMode.HTML)

@admin_only
async def edit_character_command(client: Client, message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=3)
    if len(args) < 3 or not args[1].strip().isdigit() or args[2].strip().lower() not in ["name", "rarity", "image"]:
        await message.reply_text("<b>❌ Usage: /edit &lt;character_id&gt; &lt;name|rarity|image&gt; [new_value]</b>", parse_mode=ParseMode.HTML)
        return
    char_id = int(args[1].strip())
    field = args[2].strip().lower()
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await message.reply_text("<b>❌ Character not found.</b>", parse_mode=ParseMode.HTML)
        return
    # Immediate name update if new name is provided
    if field == "name" and len(args) >= 4:
        new_name = args[3].strip()
        if len(new_name) < 2 or len(new_name) > 50:
            await message.reply_text("<b>❌ Name must be between 2 and 50 characters.</b>", parse_mode=ParseMode.HTML)
            return
        await db.edit_character(char_id, {"name": new_name})
        rarity_emoji = RARITIES[character["rarity"]]["emoji"]
        await message.reply_photo(
            photo=character["img_url"],
            caption=(
                f"<b>✅ Character name updated!</b>\n\n"
                f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
                f"<b>👤 Name:</b> {new_name}\n"
                f"<b>✨ Rarity:</b> {rarity_emoji} {character['rarity']}\n"
            )
        )
        return
    # Immediate rarity update if new rarity is provided
    if field == "rarity" and len(args) >= 4:
        rarity_input = args[3].strip().lower()
        matched_rarity = None
        for r in RARITIES:
            if rarity_input == r.lower() or r.lower().startswith(rarity_input):
                matched_rarity = r
                break
        if not matched_rarity:
            for r in RARITIES:
                if rarity_input in r.lower():
                    matched_rarity = r
                    break
        if not matched_rarity:
            await message.reply_text(f"<b>❌ Invalid rarity! Choose one of:</b> {' | '.join(RARITIES.keys())}", parse_mode=ParseMode.HTML)
            return
        await db.edit_character(char_id, {"rarity": matched_rarity})
        rarity_emoji = RARITIES[matched_rarity]["emoji"]
        await message.reply_photo(
            photo=character["img_url"],
            caption=(
                f"<b>✅ Character rarity updated!</b>\n\n"
                f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
                f"<b>👤 Name:</b> {character['name']}\n"
                f"<b>✨ Rarity:</b> {rarity_emoji} {matched_rarity}\n"
            )
        )
        return
    # Immediate image update if this is a reply to a photo
    if field == "image" and message.reply_to_message and message.reply_to_message.photo:
        file_id = message.reply_to_message.photo.file_id
        img_url = await upload_to_imgbb(file_id, client)
        if not img_url:
            await message.reply_text(f"<b>❌ Failed to upload image to ImgBB.</b>\n\n{get_upload_error_message()}", parse_mode=ParseMode.HTML)
            return
        # Show preview and ask for confirmation
        PENDING_EDIT[user_id] = {"id": char_id, "character": character, "field": field, "new_value": {"file_id": file_id, "img_url": img_url}, "step": "confirm"}
        await message.reply_photo(
            photo=file_id,
            caption=(
                f"<b>Preview Edit</b>\n\n"
                f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
                f"<b>👤 Name:</b> {character['name']}\n"
                f"<b>✨ Rarity:</b> {RARITIES[character['rarity']]['emoji']} {character['rarity']}\n\n"
                f"Confirm change?"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="edit_confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")],
            ])
        )
        return
    # Improved image flow: prompt for reply
    if field == "image":
        prompt = await message.reply_photo(
            photo=character["img_url"],
            caption=(
                f"<b>🖼 Edit Image</b>\n\n"
                f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
                f"<b>👤 Name:</b> {character['name']}\n"
                f"<b>✨ Rarity:</b> {RARITIES[character['rarity']]['emoji']} {character['rarity']}\n\n"
                f"Reply to this message with the new image."
            ),
        )
        PENDING_EDIT[user_id] = {"id": char_id, "character": character, "field": field, "step": "awaiting_image_reply", "prompt_msg_id": prompt.message_id}
        return
    # Fallback to old prompt for name
    PENDING_EDIT[user_id] = {"id": char_id, "character": character, "field": field, "step": "awaiting_value"}
    if field == "name":
        await message.reply_text("<b>Send the new name:</b>", parse_mode=ParseMode.HTML)

# Callback for edit choice
async def edit_character_choice_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    state = PENDING_EDIT.get(user_id)
    if not state or state.get("step") != "choice":
        await callback_query.answer("No edit in progress.", show_alert=True)
        return
    if callback_query.data == "edit_name":
        PENDING_EDIT[user_id]["step"] = "edit_name"
        await callback_query.answer()
        await callback_query.message.reply_text("<b>Send the new name:</b>", parse_mode=ParseMode.HTML)
    elif callback_query.data == "edit_rarity":
        PENDING_EDIT[user_id]["step"] = "edit_rarity"
        await callback_query.answer()
        await callback_query.message.reply_text("<b>Send the new rarity:</b>", parse_mode=ParseMode.HTML)
    elif callback_query.data == "edit_image":
        PENDING_EDIT[user_id]["step"] = "edit_image"
        await callback_query.answer()
        await callback_query.message.reply_text("<b>Reply with the new image:</b>", parse_mode=ParseMode.HTML)
    elif callback_query.data == "edit_cancel":
        PENDING_EDIT.pop(user_id, None)
        await callback_query.answer("Edit cancelled.", show_alert=False)
        await callback_query.message.edit_caption("<b>❌ Edit cancelled.</b>", parse_mode=ParseMode.HTML)

# Handler for new name
async def edit_character_name_handler(client: Client, message: Message):
    user_id = message.from_user.id
    state = PENDING_EDIT.get(user_id)
    if not state or state.get("step") != "awaiting_value" or state.get("field") != "name":
        return
    new_name = message.text.strip()
    if len(new_name) < 2 or len(new_name) > 50:
        await message.reply_text("<b>❌ Name must be between 2 and 50 characters.</b>", parse_mode=ParseMode.HTML)
        return
    PENDING_EDIT[user_id]["new_value"] = new_name
    PENDING_EDIT[user_id]["step"] = "confirm"
    char = state["character"]
    await message.reply_photo(
        photo=char["img_url"],
        caption=(
            f"<b>Preview Edit</b>\n\n"
            f"<b>🆔 ID:</b> <code>{state['id']}</code>\n"
            f"<b>👤 Name:</b> {new_name}\n"
            f"<b>✨ Rarity:</b> {RARITIES[char['rarity']]['emoji']} {char['rarity']}\n\n"
            f"Confirm change?"
        ),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="edit_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")],
        ])
    )

# Handler for new rarity
async def edit_character_rarity_handler(client: Client, message: Message):
    user_id = message.from_user.id
    state = PENDING_EDIT.get(user_id)
    if not state or state.get("step") != "awaiting_value" or state.get("field") != "rarity":
        return
    rarity_input = message.text.strip().lower()
    matched_rarity = None
    for r in RARITIES:
        if rarity_input == r.lower() or r.lower().startswith(rarity_input):
            matched_rarity = r
            break
    if not matched_rarity:
        for r in RARITIES:
            if rarity_input in r.lower():
                matched_rarity = r
                break
    if not matched_rarity:
        await message.reply_text(f"<b>❌ Invalid rarity! Choose one of:</b> {' | '.join(RARITIES.keys())}", parse_mode=ParseMode.HTML)
        return
    PENDING_EDIT[user_id]["new_value"] = matched_rarity
    PENDING_EDIT[user_id]["step"] = "confirm"
    char = state["character"]
    await message.reply_photo(
        photo=char["img_url"],
        caption=(
            f"<b>Preview Edit</b>\n\n"
            f"<b>🆔 ID:</b> <code>{state['id']}</code>\n"
            f"<b>👤 Name:</b> {char['name']}\n"
            f"<b>✨ Rarity:</b> {RARITIES[matched_rarity]['emoji']} {matched_rarity}\n\n"
            f"Confirm change?"
        ),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="edit_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")],
        ])
    )

# Handler for new image (reply to prompt)
async def edit_character_image_handler(client: Client, message: Message):
    user_id = message.from_user.id
    state = PENDING_EDIT.get(user_id)
    if not state or state.get("step") != "awaiting_image_reply" or state.get("field") != "image":
        return
    # Only accept if this is a reply to the bot's prompt
    if not message.reply_to_message or message.reply_to_message.message_id != state.get("prompt_msg_id"):
        return
    if not message.photo:
        await message.reply_text("<b>❌ Please reply with a photo.</b>", parse_mode=ParseMode.HTML)
        return
    file_id = message.photo.file_id
    img_url = await upload_to_imgbb(file_id, client)
    if not img_url:
        await message.reply_text(f"<b>❌ Failed to upload image to ImgBB.</b>\n\n{get_upload_error_message()}", parse_mode=ParseMode.HTML)
        return
    PENDING_EDIT[user_id]["new_value"] = {"file_id": file_id, "img_url": img_url}
    PENDING_EDIT[user_id]["step"] = "confirm"
    char = state["character"]
    await message.reply_photo(
        photo=img_url,
        caption=(
            f"<b>Preview Edit</b>\n\n"
            f"<b>🆔 ID:</b> <code>{state['id']}</code>\n"
            f"<b>👤 Name:</b> {char['name']}\n"
            f"<b>✨ Rarity:</b> {RARITIES[char['rarity']]['emoji']} {char['rarity']}\n\n"
            f"Confirm change?"
        ),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="edit_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")],
        ])
    )

# Confirm/cancel edit
async def edit_character_confirm_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    state = PENDING_EDIT.get(user_id)
    if not state or state.get("step") != "confirm":
        await callback_query.answer("No edit to confirm.", show_alert=True)
        return
    db = get_database()
    char_id = state["id"]
    field = state["field"]
    new_value = state["new_value"]
    update = {}
    if field == "name":
        update["name"] = new_value
    elif field == "rarity":
        update["rarity"] = new_value
    elif field == "image":
        update.update(new_value)
    await db.edit_character(char_id, update)
    char = await db.get_character(char_id)
    rarity_emoji = RARITIES[char["rarity"]]["emoji"]
    await callback_query.message.edit_caption(
        f"<b>✅ Character updated!</b>\n\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>👤 Name:</b> {char['name']}\n"
        f"<b>✨ Rarity:</b> {rarity_emoji} {char['rarity']}\n"
    )
    PENDING_EDIT.pop(user_id, None)
    admin_user = await client.get_users(user_id)
    log_caption = (
        f"<b>✏️ Character edited</b>\n"
        f"<b>👤 Name:</b> {char.get('name', '-') }\n"
        f"<b>🆔 ID:</b> <code>{char.get('character_id', char.get('_id', '-'))}</code>\n"
        f"<b>💎 Rarity:</b> {char.get('rarity', '-') }\n"
        f"<b>👮‍♂️ Edited by:</b> {admin_user.mention if hasattr(admin_user, 'mention') else admin_user.first_name} (<code>{admin_user.id}</code>)"
    )
    try:
        await client.send_photo(
            chat_id=DROPTIME_LOG_CHANNEL,
            photo=char.get('img_url', char.get('file_id', None)),
            caption=log_caption
        )
    except Exception as e:
        print(f"Failed to send character edit log to droptime channel: {e}")
        # Don't crash the bot, just log the error

async def edit_character_cancel_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id in PENDING_EDIT:
        PENDING_EDIT.pop(user_id, None)
    await callback_query.message.edit_caption("<b>❌ Edit cancelled.</b>")

# Callback for rarity button selection
async def edit_rarity_button_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if not data.startswith("edit_rarity_"):
        return
    parts = data.split("_", 3)
    if len(parts) < 4:
        await callback_query.answer("Invalid rarity selection.", show_alert=True)
        return
    char_id = int(parts[2])
    rarity = parts[3]
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await callback_query.answer("Character not found.", show_alert=True)
        return
    PENDING_EDIT[user_id] = {"id": char_id, "character": character, "field": "rarity", "new_value": rarity, "step": "confirm"}
    await callback_query.message.edit_caption(
        f"<b>Preview Edit</b>\n\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>👤 Name:</b> {character['name']}\n"
        f"<b>✨ Rarity:</b> {RARITIES[rarity]['emoji']} {rarity}\n\n"
        f"Confirm change?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="edit_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")],
        ])
    )

# Register the new /add command and confirm/cancel handlers

def register_upload_handlers(app: Client):
    app.add_handler(filters.command("add"), add_character_command)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data == "addchar_confirm"), add_character_confirm_callback)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data == "addchar_cancel"), add_character_cancel_callback)
    app.add_handler(filters.command("edit"), edit_character_command)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data.startswith("edit_")), edit_character_choice_callback)
    app.add_handler(filters.text & filters.create(lambda _, m, __: PENDING_EDIT.get(m.from_user.id, {}).get("step") == "edit_name"), edit_character_name_handler)
    app.add_handler(filters.text & filters.create(lambda _, m, __: PENDING_EDIT.get(m.from_user.id, {}).get("step") == "edit_rarity"), edit_character_rarity_handler)
    app.add_handler(filters.photo & filters.create(lambda _, m, __: PENDING_EDIT.get(m.from_user.id, {}).get("step") == "edit_image"), edit_character_image_handler)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data == "edit_confirm"), edit_character_confirm_callback)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data == "edit_cancel"), edit_character_cancel_callback)
    app.on_callback_query(filters.regex(r"^edit_rarity_\d+_.+"))(edit_rarity_button_callback)
    app.add_handler(filters.command("delete"), delete_character_command)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data.startswith("delete_confirm_")), delete_character_confirm_callback)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data.startswith("delete_cancel_")), delete_character_cancel_callback)
    app.add_handler(filters.command("reset"), reset_character_command)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data.startswith("reset_confirm_")), reset_character_confirm_callback)
    app.add_handler(filters.callback_query & filters.create(lambda _, cq: cq.data.startswith("reset_cancel_")), reset_character_cancel_callback)

async def upload_to_imgbb(file_id: str, client) -> str:
    """Upload image to ImgBB."""
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Download file from Telegram
            file = await asyncio.wait_for(client.download_media(file_id), timeout=120)

            # Load file bytes and filename
            if isinstance(file, str):
                filename = os.path.basename(file)
                with open(file, 'rb') as f:
                    file_bytes = f.read()
            elif hasattr(file, 'read'):
                file.seek(0)
                file_bytes = file.read()
                filename = 'upload.jpg'
            else:
                raise Exception("Unsupported file type")

            # Guess content type
            content_type, _ = mimetypes.guess_type(filename)
            if not content_type:
                content_type = 'application/octet-stream'

            # Prepare form data for ImgBB
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(ssl=False, limit=100, limit_per_host=30)
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                data = aiohttp.FormData()
                data.add_field('image', file_bytes, filename=filename, content_type=content_type)

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                # ImgBB API key
                api_key = "0e41b21a7b6202d84e85ed9fd7d300ff"
                data.add_field('key', api_key)

                async with session.post('https://api.imgbb.com/1/upload', data=data, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        if 'data' in result and 'url' in result['data']:
                            return result['data']['url']
                        else:
                            raise Exception(f"ImgBB API returned unexpected response: {result}")
                    else:
                        raise Exception(f"ImgBB API returned status {response.status}")

        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                print(f"[Retry {attempt+1}] Timeout while downloading Telegram media. Retrying...")
                await asyncio.sleep(2)
                continue
            print("Download timed out after multiple attempts.")
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[Retry {attempt+1}] Error uploading to ImgBB: {e}. Retrying...")
                await asyncio.sleep(2)
                continue
            print(f"Error uploading to ImgBB: {e}")
            return None

    return None

def get_upload_error_message():
    """Get a user-friendly error message for upload failures."""
    return (
        "<b>❌ Upload failed!</b>\n\n"
        "This could be due to:\n"
        "• Network connectivity issues\n"
        "• Large file size (try a smaller image)\n"
        "• Temporary server maintenance\n"
        "• Firewall or proxy blocking uploads\n\n"
        "Please try again in a few minutes or use a smaller image file."
    )



@admin_only
async def delete_character_command(client: Client, message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.reply_text("<b>❌ Usage: /delete &lt;character_id&gt;</b>", parse_mode=ParseMode.HTML)
        return
    char_id = int(args[1].strip())
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await message.reply_text("<b>❌ Character not found.</b>", parse_mode=ParseMode.HTML)
        return
    rarity_emoji = RARITIES[character["rarity"]]["emoji"]
    keyboard = [
        [InlineKeyboardButton("✅ Confirm Delete", callback_data=f"delete_confirm_{char_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"delete_cancel_{char_id}")],
    ]
    await message.reply_photo(
        photo=character["img_url"],
        caption=(
            f"<b>⚠️ Confirm Deletion</b>\n\n"
            f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
            f"<b>👤 Name:</b> {character['name']}\n"
            f"<b>✨ Rarity:</b> {rarity_emoji} {character['rarity']}\n\n"
            f"Are you sure you want to delete this character? This will remove it from all users' collections as well."
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

# Callback for delete confirmation
@admin_only
async def delete_character_confirm_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if not data.startswith("delete_confirm_"):
        return
    char_id = int(data.split("_", 2)[2])
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await callback_query.answer("Character not found.", show_alert=True)
        return
    # Remove character from all users' collections and delete character from characters collection
    await db.delete_character(char_id)
    await callback_query.message.edit_caption(
        f"<b>✅ Character deleted!</b>\n\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>👤 Name:</b> {character['name']}\n",
        parse_mode=ParseMode.HTML
    )
    admin_user = await client.get_users(user_id)
    log_caption = (
        f"<b>🗑️ Character deleted</b>\n"
        f"<b>👤 Name:</b> {character.get('name', '-') }\n"
        f"<b>🆔 ID:</b> <code>{character.get('character_id', character.get('_id', '-'))}</code>\n"
        f"<b>💎 Rarity:</b> {character.get('rarity', '-') }\n"
        f"<b>👮‍♂️ Deleted by:</b> {admin_user.mention if hasattr(admin_user, 'mention') else admin_user.first_name} (<code>{admin_user.id}</code>)"
    )
    try:
        await client.send_photo(
            chat_id=DROPTIME_LOG_CHANNEL,
            photo=character.get('img_url', character.get('file_id', None)),
            caption=log_caption,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Failed to send character delete log to droptime channel: {e}")
        # Don't crash the bot, just log the error

# Callback for delete cancel
@admin_only
async def delete_character_cancel_callback(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    if not data.startswith("delete_cancel_"):
        return
    await callback_query.message.edit_caption("<b>❌ Deletion cancelled.</b>")

@admin_only
async def reset_character_command(client: Client, message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.reply_text("<b>❌ Usage: /reset &lt;character_id&gt;</b>", parse_mode=ParseMode.HTML)
        return
    char_id = int(args[1].strip())
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await message.reply_text("<b>❌ Character not found.</b>", parse_mode=ParseMode.HTML)
        return
    rarity_emoji = RARITIES[character["rarity"]]["emoji"]
    keyboard = [
        [InlineKeyboardButton("✅ Confirm Reset", callback_data=f"reset_confirm_{char_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"reset_cancel_{char_id}")],
    ]
    await message.reply_photo(
        photo=character["img_url"],
        caption=(
            f"<b>⚠️ Confirm Reset</b>\n\n"
            f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
            f"<b>👤 Name:</b> {character['name']}\n"
            f"<b>✨ Rarity:</b> {rarity_emoji} {character['rarity']}\n\n"
            f"Are you sure you want to reset this character? This will remove it from all users' collections globally, but the character will remain in the database."
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

# Callback for reset confirmation
@admin_only
async def reset_character_confirm_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if not data.startswith("reset_confirm_"):
        return
    char_id = int(data.split("_", 2)[2])
    db = get_database()
    character = await db.get_character(char_id)
    if not character:
        await callback_query.answer("Character not found.", show_alert=True)
        return
    # Remove character from all users' collections (PostgreSQL)
    await db.reset_character_from_collections(char_id)
    await callback_query.message.edit_caption(
        f"<b>✅ Character reset globally!</b>\n\n"
        f"<b>🆔 ID:</b> <code>{char_id}</code>\n"
        f"<b>👤 Name:</b> {character['name']}\n",
        parse_mode=ParseMode.HTML
    )
    admin_user = await client.get_users(user_id)
    log_caption = (
        f"<b>♻️ Character reset globally</b>\n"
        f"<b>👤 Name:</b> {character.get('name', '-') }\n"
        f"<b>🆔 ID:</b> <code>{character.get('character_id', character.get('_id', '-'))}</code>\n"
        f"<b>💎 Rarity:</b> {character.get('rarity', '-') }\n"
        f"<b>👮‍♂️ Reset by:</b> {admin_user.mention if hasattr(admin_user, 'mention') else admin_user.first_name} (<code>{admin_user.id}</code>)"
    )
    try:
        await client.send_photo(
            chat_id=DROPTIME_LOG_CHANNEL,
            photo=character.get('img_url', character.get('file_id', None)),
            caption=log_caption,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Failed to send character reset log to droptime channel: {e}")
        # Don't crash the bot, just log the error

# Callback for reset cancel
@admin_only
async def reset_character_cancel_callback(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    if not data.startswith("reset_cancel_"):
        return
    await callback_query.message.edit_caption("<b>❌ Reset cancelled.</b>")
