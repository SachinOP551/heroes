#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Infinity Stones Module for Marvel Collector Bot
Allows users to collect the 6 infinity stones with extremely rare drop rates
Now integrated with the drop system for automatic drops
"""

import random
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, CallbackQuery
from pyrogram.enums import ChatType
from modules.postgres_database import get_postgres_pool
from modules.decorators import auto_register_user
from modules.postgres_database import get_database

# Gauntlet constants
INFINITY_GAUNTLET_TYPE = "infinity_gauntlet"
INFINITY_GAUNTLET_NAME = "Infinity Gauntlet"

# Infinity Stones Configuration
INFINITY_STONES = {
    "space_stone": {
        "name": "Space Stone",
        "emoji": "🔵",
        "drop_rate": 0.0001,  # 0.01% - very rare
        "character_id": "infinity_space_stone",  # Unique identifier for drop system
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/Gf63wnmG/c65357ea3de22a031c55560bb041fe9c.jpg",  # Blue gemstone image
        "drop_caption": "🌌 Tʜᴇ Tᴇssᴇʀᴀᴄᴛ ᴡʜɪsᴘᴇʀs ᴏꜰ ᴅɪsᴛᴀɴᴛ ᴡᴏʀʟᴅs…\n✴️ <code>/collect name</code> — Bᴇɴᴅ ʀᴇᴀʟɪᴛʏ's ᴇᴅɢᴇs."
    },
    "mind_stone": {
        "name": "Mind Stone",
        "emoji": "🟡",
        "drop_rate": 0.00001,  # 0.001% - very rare
        "character_id": "infinity_mind_stone",
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/206kBgrj/photo-2025-08-13-12-49-18.jpg",  # Yellow gemstone image
        "drop_caption": "🧠 Tʜᴇ ᴍɪɴᴅ ᴜɴʟᴇᴀsʜᴇs ɪᴛs ᴘᴏᴡᴇʀ…\n✴️ <code>/collect name</code>— Uɴʟᴏᴄᴋ ᴛʜᴇ ᴘsʏᴄʜɪᴄ ʀᴇᴀʟᴍ."
    },
    "soul_stone": {
        "name": "Soul Stone",
        "emoji": "🟠",
        "drop_rate": 0.00001,  # 0.001% - extremely rare
        "character_id": "infinity_soul_stone",
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/W4J5Jv3P/photo-2025-08-14-12-27-32.jpg",  # Orange gemstone image
        "drop_caption": "💀 Tʜᴇ sᴏᴜʟ ᴏꜰ ᴛʜᴇ ᴜɴɪᴠᴇʀsᴇ ᴄᴀʟʟs…\n✴️ <code>/collect name</code> — Hᴀʀɴᴇss ᴛʜᴇ ᴘᴏᴡᴇʀ ᴏꜰ ʟɪꜰᴇ ᴀɴᴅ ᴅᴇᴀᴛʜ."
    },
    "time_stone": {
        "name": "Time Stone",
        "emoji": "🟢",
        "drop_rate": 0.00001,  # 0.01% - very rare
        "character_id": "infinity_time_stone",
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/xqfN3SLZ/399d20cd8110b92bc1afcdfb21cd1f56.jpg",  # Green gemstone image
        "drop_caption": "⏰ Tɪᴍᴇ ɪs ᴀɴ ɪʟʟᴜsɪᴏɴ ᴏꜰ ᴛʜᴇ ᴍɪɴᴅ…\n✴️ <code>/collect name</code> — Mᴀɴɪᴘᴜʟᴀᴛᴇ ᴛʜᴇ ᴠᴇʀʏ ᴇssᴇɴᴄᴇ ᴏꜰ ᴛɪᴍᴇ."
    },
    "power_stone": {
        "name": "Power Stone",
        "emoji": "🟣",
        "drop_rate": 0.0001,  # 0.01% - very rare
        "character_id": "infinity_power_stone",
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/cKzMc8m6/63782e44c04907ef8a921e2bc0c346c8.jpg",  # Purple gemstone image
        "drop_caption": "⚡ Tʜᴇ ᴘᴏᴡᴇʀ ᴏꜰ ᴛʜᴇ ᴜɴɪᴠᴇʀsᴇ ᴜɴʟᴇᴀsʜᴇs…\n✴️ <code>/collect name</code> — Eᴍʙᴏᴅʏ ᴛʜᴇ sᴛʀᴇɴɢᴛʜ ᴏꜰ ᴛʜᴇ ᴄᴏsᴍᴏs."
    },
    "reality_stone": {
        "name": "Reality Stone",
        "emoji": "🔴",
        "drop_rate": 0.0001,  # 0.01% - very rare
        "character_id": "infinity_reality_stone",
        "is_infinity_stone": True,
        "img_url": "https://i.ibb.co/W4Ksh0p9/66b421726d71a8f612aaec5b5c2e26cb.jpg",  # Red gemstone image
        "drop_caption": "🌍 Rᴇᴀʟɪᴛʏ ɪs ᴀɴ ɪʟʟᴜsɪᴏɴ ᴏꜰ ᴛʜᴇ ᴍɪɴᴅ…\n✴️ <code>/collect name</code> — Rᴇᴡʀɪᴛᴇ ᴛʜᴇ ᴠᴇʀʏ ʟᴀᴡs ᴏꜰ ᴘʜʏsɪᴄs."
    }
}

# Global variable to track active infinity stone drops
active_infinity_stone_drops = {}

# Global variable to track active snap sessions (user_id -> timestamp)
# REMOVED: active_snap_sessions = {}

# Group restriction for infinity stone drops
INFINITY_STONES_ALLOWED_GROUP_ID = -1002558794123

# Global variable to track recent infinity stone drops (chat_id -> {stone_type: timestamp})
recent_infinity_stone_drops = {}

async def create_items_table():
    """Create the items table if it doesn't exist"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_items (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    item_type VARCHAR(50) NOT NULL,
                    item_name VARCHAR(100) NOT NULL,
                    quantity INTEGER DEFAULT 1,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source VARCHAR(50) DEFAULT 'collected',
                    UNIQUE(user_id, item_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            # Create index for better performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_items_user_id ON user_items(user_id)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_items_type ON user_items(item_type)
            """)
            
        print("✅ Infinity Stones items table created successfully")
    except Exception as e:
        print(f"❌ Error creating items table: {e}")

async def get_user_items(user_id: int) -> List[Dict]:
    """Get all items owned by a user"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT item_type, item_name, quantity, collected_at, source
                FROM user_items 
                WHERE user_id = $1
                ORDER BY collected_at DESC
            """, user_id)
            
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"❌ Error getting user items: {e}")
        return []

async def add_item_to_user(user_id: int, item_type: str, item_name: str, source: str = "collected") -> bool:
    """Add an item to a user's inventory"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            # Try to insert new item, if it exists update quantity
            await conn.execute("""
                INSERT INTO user_items (user_id, item_type, item_name, quantity, source)
                VALUES ($1, $2, $3, 1, $4)
                ON CONFLICT (user_id, item_type) 
                DO UPDATE SET 
                    quantity = user_items.quantity + 1,
                    collected_at = CURRENT_TIMESTAMP
            """, user_id, item_type, item_name, source)
            
        return True
    except Exception as e:
        print(f"❌ Error adding item to user: {e}")
        return False

async def check_item_ownership(user_id: int, item_type: str) -> bool:
    """Check if a user owns a specific item"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 1 FROM user_items 
                WHERE user_id = $1 AND item_type = $2
            """, user_id, item_type)
            
            return row is not None
    except Exception as e:
        print(f"❌ Error checking item ownership: {e}")
        return False

async def has_infinity_gauntlet(user_id: int) -> bool:
    """Check if user owns the Infinity Gauntlet"""
    return await check_item_ownership(user_id, INFINITY_GAUNTLET_TYPE)

async def check_user_tokens(user_id: int, required_amount: int) -> Tuple[bool, int]:
    """Check if user has enough tokens and return current balance"""
    try:
        db = get_database()
        user = await db.get_user(user_id)
        if not user:
            return False, 0
        
        current_tokens = user.get('wallet', 0)
        has_enough = current_tokens >= required_amount
        return has_enough, current_tokens
    except Exception as e:
        print(f"❌ Error checking user tokens: {e}")
        return False, 0

async def check_user_shards(user_id: int, required_amount: int) -> Tuple[bool, int]:
    """Check if user has enough shards and return current balance"""
    try:
        db = get_database()
        user = await db.get_user(user_id)
        if not user:
            return False, 0
        
        current_shards = user.get('shards', 0)
        has_enough = current_shards >= required_amount
        return has_enough, current_shards
    except Exception as e:
        print(f"❌ Error checking user shards: {e}")
        return False, 0

async def deduct_crafting_resources(user_id: int, tokens_amount: int, shards_amount: int) -> bool:
    """Deduct tokens and shards for crafting the Infinity Gauntlet"""
    try:
        db = get_database()
        user = await db.get_user(user_id)
        if not user:
            return False
        
        current_tokens = user.get('wallet', 0)
        current_shards = user.get('shards', 0)
        
        # Check if user has enough resources
        if current_tokens < tokens_amount or current_shards < shards_amount:
            return False
        
        # Deduct the resources
        new_tokens = current_tokens - tokens_amount
        new_shards = current_shards - shards_amount
        
        await db.update_user(user_id, {
            'wallet': new_tokens,
            'shards': new_shards
        })
        
        return True
    except Exception as e:
        print(f"❌ Error deducting crafting resources: {e}")
        return False

async def get_infinity_stones_collection(user_id: int) -> Dict[str, bool]:
    """Get the user's infinity stones collection status"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT item_type FROM user_items 
                WHERE user_id = $1 AND item_type LIKE '%_stone'
            """, user_id)
            
            owned_stones = {row['item_type']: True for row in rows}
            
            # Create complete collection status
            collection_status = {}
            for stone_type in INFINITY_STONES.keys():
                collection_status[stone_type] = owned_stones.get(stone_type, False)
            
            return collection_status
    except Exception as e:
        print(f"❌ Error getting infinity stones collection: {e}")
        return {stone_type: False for stone_type in INFINITY_STONES.keys()}

async def attempt_stone_collection(user_id: int, stone_name: str, chat_id: int) -> Tuple[bool, str, Optional[Dict]]:
    """Attempt to collect an infinity stone"""
    # Normalize stone name
    stone_name = stone_name.lower().replace(" ", "_")
    
    if stone_name not in INFINITY_STONES:
        return False, "❌ Invalid infinity stone name!", None
    
    # Require Infinity Gauntlet first
    if not await has_infinity_gauntlet(user_id):
        return False, (
            "🧤 You must craft the <b>Infinity Gauntlet</b> before collecting stones.\n\n"
            "Use <code>/craftgauntlet</code> to forge it."), None
    
    # Check if user already owns this stone
    already_owned = await check_item_ownership(user_id, stone_name)
    if already_owned:
        stone_info = INFINITY_STONES[stone_name]
        return False, f"❌ You already own the {stone_info['emoji']} {stone_info['name']}!", None
    
    # Check if a valid drop exists for this stone
    if not await is_stone_drop_valid(chat_id, stone_name):
        return False, (
            "💫 The drop for this Infinity Stone has expired or is not available.\n\n"
            "Please wait for a new drop or try collecting it manually."
        ), None
    
    # Calculate drop chance with extremely rare probability
    stone_info = INFINITY_STONES[stone_name]
    drop_chance = stone_info['drop_rate']
    
    # Generate random number between 0 and 1
    random_value = random.random()
    
    if random_value <= drop_chance:
        # Success! Add stone to user's inventory
        success = await add_item_to_user(user_id, stone_name, stone_info['name'])
        if success:
            # Clear the drop after successful collection
            if chat_id in recent_infinity_stone_drops and stone_name in recent_infinity_stone_drops[chat_id]:
                del recent_infinity_stone_drops[chat_id][stone_name]
                if not recent_infinity_stone_drops[chat_id]:
                    del recent_infinity_stone_drops[chat_id]
            
            # Build compact UI message with progress
            try:
                collection_status = await get_infinity_stones_collection(user_id)
                owned_count = sum(collection_status.values())
                total_stones = len(INFINITY_STONES)
            except Exception:
                owned_count = 0
                total_stones = len(INFINITY_STONES)
            ui_message = (
                "╔══════════════════════╗\n"
                "   🎉 <b>INFINITY STONE</b> 🎉\n\n"
                f"        {stone_info['emoji']} {stone_info['name']}\n"
                "╚══════════════════════╝\n"
                f"👑 Progress towards the Supreme: {owned_count}/{total_stones}"
            )
            return True, ui_message, stone_info
        else:
            return False, "❌ Error adding stone to inventory. Please try again.", None
    else:
        # Failed to collect
        return False, f"💫 The {stone_info['emoji']} {stone_info['name']} eludes you...\n\n**Drop Rate:** {drop_chance * 100:.4f}%\n**Try again later!**", None

async def attempt_infinity_stone_drop_collection(user_id: int, stone_character_id: str) -> Tuple[bool, str, Optional[Dict]]:
    """Attempt to collect an infinity stone from a drop"""
    # Require Infinity Gauntlet first
    if not await has_infinity_gauntlet(user_id):
        return False, (
            "🧤 You must craft the <b>Infinity Gauntlet</b> before collecting stones.\n\n"
            "Use <code>/craftgauntlet</code> to forge it."), None
    # Find the stone by character_id
    stone_type = None
    for st, stone_info in INFINITY_STONES.items():
        if stone_info['character_id'] == stone_character_id:
            stone_type = st
            break
    
    if not stone_type:
        return False, "❌ Invalid infinity stone!", None
    
    stone_info = INFINITY_STONES[stone_type]
    
    # Check if user already owns this stone
    already_owned = await check_item_ownership(user_id, stone_type)
    if already_owned:
        return False, f"❌ You already own the {stone_info['emoji']} {stone_info['name']}!", None
    
    # Success! Add stone to user's inventory (drop collection is always successful)
    success = await add_item_to_user(user_id, stone_type, stone_info['name'], "dropped")
    if success:
        # Build compact UI message with progress
        try:
            collection_status = await get_infinity_stones_collection(user_id)
            owned_count = sum(collection_status.values())
            total_stones = len(INFINITY_STONES)
        except Exception:
            owned_count = 0
            total_stones = len(INFINITY_STONES)
        ui_message = (
                "╔══════════════════════╗\n"
                "   🎉 <b>INFINITY STONE</b> 🎉\n\n"
                f"        {stone_info['emoji']} {stone_info['name']}\n"
                "╚══════════════════════╝\n"
                f"👑 Progress towards the Supreme: {owned_count}/{total_stones}"
            )
        return True, ui_message, stone_info
    else:
        return False, "❌ Error adding stone to inventory. Please try again.", None

async def attempt_stone_collection_by_short_name(user_id: int, short_name: str, chat_id: int) -> Tuple[bool, str, Optional[Dict]]:
    """Attempt to collect an infinity stone using short name (e.g., 'space' for 'Space Stone').
    Strictly requires that the specific stone is currently dropped in this chat.
    """
    # Require Infinity Gauntlet first
    if not await has_infinity_gauntlet(user_id):
        return False, (
            "🧤 You must craft the <b>Infinity Gauntlet</b> before collecting stones.\n\n"
            "Use <code>/craftgauntlet</code> to forge it."), None

    # Check if user is in the allowed group for infinity stones
    if not await is_infinity_stones_allowed_in_chat(chat_id):
        return False, (
            "❌ <b>Infinity Stones are not available in this group!</b>\n\n"
            f"Infinity Stones only drop in group <code>{INFINITY_STONES_ALLOWED_GROUP_ID}</code>\n\n"
            "Use <code>/infinitygroup</code> for more information."
        ), None

    # Normalize and map short name to exact stone type
    short_key = short_name.lower().strip()
    short_to_type = {
        'space': 'space_stone',
        'mind': 'mind_stone',
        'soul': 'soul_stone',
        'time': 'time_stone',
        'power': 'power_stone',
        'reality': 'reality_stone',
    }
    stone_type = short_to_type.get(short_key)
    if not stone_type or stone_type not in INFINITY_STONES:
        return False, "❌ Invalid infinity stone name!", None

    stone_info = INFINITY_STONES[stone_type]

    # Enforce that this exact stone has a valid, recent drop in this chat
    if not await is_stone_drop_valid(chat_id, stone_type):
        # Try to hint which stone is currently active in this chat, if any
        active_hint = ""
        try:
            active_drop = await get_active_infinity_stone_drop(chat_id)
            if active_drop and active_drop.get('stone_type'):
                active_st = active_drop['stone_type']
                active_info = INFINITY_STONES.get(active_st, {})
                if active_info:
                    active_hint = f"\n\nCurrently active drop: {active_info.get('emoji', '💎')} {active_info.get('name', active_st.replace('_', ' ').title())}"
        except Exception:
            pass
        return False, (
            "💫 The drop for this Infinity Stone is not active in this chat or has expired." + active_hint
        ), None

    # Check if user already owns this stone
    already_owned = await check_item_ownership(user_id, stone_type)
    if already_owned:
        return False, f"❌ You already own the {stone_info['emoji']} {stone_info['name']}!", None

    # Success: when the correct stone is actively dropped, collecting is guaranteed
    success = await add_item_to_user(user_id, stone_type, stone_info['name'])
    if success:
        # Clear the drop after successful collection
        if chat_id in recent_infinity_stone_drops and stone_type in recent_infinity_stone_drops[chat_id]:
            del recent_infinity_stone_drops[chat_id][stone_type]
            if not recent_infinity_stone_drops[chat_id]:
                del recent_infinity_stone_drops[chat_id]

        # Build compact UI message with progress
        try:
            collection_status = await get_infinity_stones_collection(user_id)
            owned_count = sum(collection_status.values())
            total_stones = len(INFINITY_STONES)
        except Exception:
            owned_count = 0
            total_stones = len(INFINITY_STONES)
        ui_message = (
                "╔══════════════════════╗\n"
                "   🎉 <b>INFINITY STONE</b> 🎉\n\n"
                f"        {stone_info['emoji']} {stone_info['name']}\n"
                "╚══════════════════════╝\n"
                f"👑 Progress towards the Supreme: {owned_count}/{total_stones}"
            )
        return True, ui_message, stone_info
    else:
        return False, "❌ Error adding stone to inventory. Please try again.", None

async def get_random_infinity_stone_for_drop() -> Optional[Dict]:
    """Get a random infinity stone for dropping (with drop rate calculation)"""
    # Calculate total drop probability
    total_probability = sum(stone['drop_rate'] for stone in INFINITY_STONES.values())
    
    # Generate random number
    random_value = random.random() * total_probability
    
    # Find which stone should be dropped
    current_probability = 0
    for stone_type, stone_info in INFINITY_STONES.items():
        current_probability += stone_info['drop_rate']
        if random_value <= current_probability:
            # Create a character-like object for the drop system
            return {
                'character_id': stone_info['character_id'],
                'name': stone_info['name'],
                'rarity': 'Mythic',  # Default rarity for infinity stones
                'is_infinity_stone': True,
                'stone_type': stone_type,
                'emoji': stone_info['emoji'],
                'drop_caption': stone_info['drop_caption'],
                'file_id': None,  # Will be set by the drop system
                'img_url': stone_info['img_url'],  # Use the image URL from config
                'is_video': False
            }
    
    return None

async def is_infinity_stone_character(character_id: str) -> bool:
    """Check if a character_id belongs to an infinity stone"""
    return any(stone['character_id'] == character_id for stone in INFINITY_STONES.values())

async def get_infinity_stone_info(character_id: str) -> Optional[Dict]:
    """Get infinity stone info by character_id"""
    for stone_type, stone_info in INFINITY_STONES.items():
        if stone_info['character_id'] == character_id:
            return {**stone_info, 'stone_type': stone_type}
    return None

async def inventory_command(client: Client, message: Message):
    """Handle the /inventory command to view user's items"""
    user_id = message.from_user.id
    
    # Get user's items
    items = await get_user_items(user_id)
    
    if not items:
        await message.reply_text(
            "🎒 <b>Your Inventory</b> 🎒\n\n"
            "You don't have any items yet.\n\n"
            "<b>To collect Infinity Stones, you need:</b>\n"
            "🧤 <b>Infinity Gauntlet</b> - 50,00,000 tokens + 15,000 shards\n\n"
        )
        return
    
    # Group items by type
    infinity_stones = [item for item in items if item['item_type'].endswith('_stone')]
    gauntlet_items = [item for item in items if item['item_type'] == INFINITY_GAUNTLET_TYPE]
    other_items = [item for item in items if not item['item_type'].endswith('_stone') and item['item_type'] != INFINITY_GAUNTLET_TYPE]
    
    inventory_text = "🎒 **Your Inventory** 🎒\n\n"
    
    # Show Infinity Gauntlet
    if gauntlet_items:
        g = gauntlet_items[0]
        collected_at = g['collected_at'].strftime("%Y-%m-%d %H:%M") if g['collected_at'] else "Unknown"
        inventory_text += "🧤 <b>Infinity Gauntlet</b>\n\n"

    # Show Infinity Stones next
    if infinity_stones:
        inventory_text += "🔱 **Infinity Stones** 🔱:\n\n"
        for item in infinity_stones:
            stone_info = INFINITY_STONES.get(item['item_type'], {})
            emoji = stone_info.get('emoji', '💎')
            name = stone_info.get('name', item['item_name'])
            quantity = item['quantity']
            source = item['source']
            collected_at = item['collected_at'].strftime("%Y-%m-%d %H:%M") if item['collected_at'] else "Unknown"
            
            inventory_text += f"{emoji} **{name}** (x{quantity})\n\n"
    
    # Show other items
    if other_items:
        inventory_text += "🎁 **Other Items:**\n"
        for item in other_items:
            quantity = item['quantity']
            source = item['source']
            collected_at = item['collected_at'].strftime("%Y-%m-%d %H:%M") if item['collected_at'] else "Unknown"
            
            inventory_text += f"📦 **{item['item_name']}** (x{quantity})\n"
            inventory_text += f"   Source: {source.title()}\n"
            inventory_text += f"   Collected: {collected_at}\n\n"
    
    # Add collection progress
    if infinity_stones:
        collection_status = await get_infinity_stones_collection(user_id)
        owned_count = sum(collection_status.values())
        total_stones = len(INFINITY_STONES)
        inventory_text += f"📊 **Infinity Stones Progress:** {owned_count}/{total_stones}\n\n"
        
        if owned_count == total_stones:
            inventory_text += "🎊 **UNIVERSE MASTER!** You have collected all Infinity Stones! 🎊\n"
        elif owned_count > 0:
            inventory_text += f"Continue your quest to collect all {total_stones} Infinity Stones!\n"
    
    # Add help text
    inventory_text += "\n💡 **Commands:**\n"
    inventory_text += "• `/collect <stone>` - Try to collect a specific stone (e.g., /collect space)\n"
    inventory_text += "• `/craftgauntlet` - Craft the Infinity Gauntlet (50L tokens + 15K shards)\n"
    
    await message.reply_text(inventory_text)

async def collect_stone_command(client: Client, message: Message):
    """Handle the /collect <stone_name> command"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Parse stone name from command
    if len(message.command) < 2:
        await message.reply_text(
            "❌ <b>Usage:</b> <code>/collect &lt;stone_name&gt;</code>\n\n"
            "<b>Available stones:</b>\n" + 
            "\n".join([f"• {stone_info['emoji']} {stone_info['name']}" for stone_info in INFINITY_STONES.values()])
        )
        return
    
    stone_name = " ".join(message.command[1:])
    
    # Check if user is in the allowed group for infinity stones
    if not await is_infinity_stones_allowed_in_chat(chat_id):
        await message.reply_text(
            "❌ <b>Infinity Stones are not available in this group!</b>\n\n"
            f"Infinity Stones only drop in group <code>{INFINITY_STONES_ALLOWED_GROUP_ID}</code>\n\n"
            "Use <code>/infinitygroup</code> for more information."
        )
        return
    
    # Attempt to collect the stone
    success, message_text, stone_info = await attempt_stone_collection(user_id, stone_name, chat_id)
    
    if success:
        # Send success message with celebration
        await message.reply_text(message_text)
        
        # Send a follow-up message about the collection
        collection_status = await get_infinity_stones_collection(user_id)
        owned_count = sum(collection_status.values())
        total_stones = len(INFINITY_STONES)
        
        if owned_count == total_stones:
            await message.reply_text(
                "🎊 <b>UNIVERSE MASTER ACHIEVEMENT UNLOCKED!</b> 🎊\n\n"
                "You have collected all 6 Infinity Stones!\n"
                "You are now the master of reality itself!\n\n"
                "Use <code>/inventory</code> to view your complete collection!"
            )
    else:
        await message.reply_text(message_text)

async def handle_infinity_stones_callback(client: Client, callback_query):
    """Handle infinity stones callback queries"""
    data = callback_query.data
    user_id = callback_query.from_user.id
    
    if data.startswith("collect_stone:"):
        stone_type = data.split(":")[1]
        
        # Attempt to collect the stone
        success, message_text, stone_info = await attempt_stone_collection(user_id, stone_type, callback_query.message.chat.id)
        
        if success:
            # Update the message with the compact UI success text
            await callback_query.edit_message_text(message_text)
            
            # Send follow-up message about collection progress
            collection_status = await get_infinity_stones_collection(user_id)
            owned_count = sum(collection_status.values())
            total_stones = len(INFINITY_STONES)
            
            if owned_count == total_stones:
                await client.send_message(
                    callback_query.message.chat.id,
                    "🎊 **UNIVERSE MASTER ACHIEVEMENT UNLOCKED!** 🎊\n\n"
                    "You have collected all 6 Infinity Stones!\n"
                    "You are now the master of reality itself!"
                )
        else:
            # Update the message with failure
            await callback_query.edit_message_text(message_text)
        
        await callback_query.answer()
        
    elif data == "refresh_infinity_stones":
        # Refresh the collection display
        collection_status = await get_infinity_stones_collection(user_id)
        owned_count = sum(collection_status.values())
        total_stones = len(INFINITY_STONES)
        
        collection_text = f"💎 **INFINITY STONES COLLECTION** 💎\n\n"
        collection_text += f"**Progress:** {owned_count}/{total_stones} stones collected\n\n"
        
        for stone_type, is_owned in collection_status.items():
            stone_info = INFINITY_STONES[stone_type]
            status_emoji = "✅" if is_owned else "❌"
            collection_text += f"{status_emoji} {stone_info['emoji']} **{stone_info['name']}**\n\n"
        
        if owned_count == total_stones:
            collection_text += "🎊 **CONGRATULATIONS!** You have collected all Infinity Stones! 🎊\n"
            collection_text += "You are now worthy of wielding the power of the universe!"
        
        # Recreate keyboard
        keyboard = []
        for stone_type, is_owned in collection_status.items():
            if not is_owned:
                stone_info = INFINITY_STONES[stone_type]
                button_text = f"💫 Collect {stone_info['emoji']} {stone_info['name']}"
                callback_data = f"collect_stone:{stone_type}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        if keyboard:
            keyboard.append([InlineKeyboardButton("🔄 Refresh Collection", callback_data="refresh_infinity_stones")])
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await callback_query.edit_message_text(collection_text, reply_markup=reply_markup)
        await callback_query.answer("Collection refreshed!")

async def get_infinity_stones_stats() -> Dict:
    """Get statistics about infinity stones collection across all users"""
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            # Get total stones collected
            total_collected = await conn.fetchval("""
                SELECT COUNT(*) FROM user_items WHERE item_type LIKE '%_stone'
            """)
            
            # Get unique users who have collected stones
            unique_collectors = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM user_items WHERE item_type LIKE '%_stone'
            """)
            
            # Get stones by type
            stone_counts = await conn.fetch("""
                SELECT item_type, COUNT(*) as count 
                FROM user_items 
                WHERE item_type LIKE '%_stone'
                GROUP BY item_type
            """)
            
            stone_stats = {row['item_type']: row['count'] for row in stone_counts}
            
            # Get users with most stones
            top_collectors = await conn.fetch("""
                SELECT u.user_id, u.username, u.first_name, COUNT(ui.item_type) as stone_count
                FROM users u
                JOIN user_items ui ON u.user_id = ui.user_id
                WHERE ui.item_type LIKE '%_stone'
                GROUP BY u.user_id, u.username, u.first_name
                ORDER BY stone_count DESC
                LIMIT 10
            """)
            
            return {
                "total_collected": total_collected,
                "unique_collectors": unique_collectors,
                "stone_stats": stone_stats,
                "top_collectors": [dict(row) for row in top_collectors]
            }
    except Exception as e:
        print(f"❌ Error getting infinity stones stats: {e}")
        return {}

async def initialize_infinity_stones():
    """Initialize the infinity stones system"""
    await create_items_table()
    print("✅ Infinity Stones system initialized successfully")

# New functions for drop system integration
async def is_infinity_stones_allowed_in_chat(chat_id: int) -> bool:
    """Check if infinity stones are allowed to drop in this chat"""
    return chat_id == INFINITY_STONES_ALLOWED_GROUP_ID

async def should_drop_infinity_stone(chat_id: int = None) -> bool:
    """Determine if an infinity stone should be dropped based on drop rates and chat restrictions"""
    # If chat_id is provided, check if infinity stones are allowed in this chat
    if chat_id is not None and not await is_infinity_stones_allowed_in_chat(chat_id):
        return False
    
    total_probability = sum(stone['drop_rate'] for stone in INFINITY_STONES.values())
    return random.random() < total_probability

async def create_infinity_stone_drop_message(stone_info: Dict, chat_id: int = None) -> str:
    """Create a drop message for an infinity stone"""
    base = stone_info.get('drop_caption', f"💎 **{stone_info['name']}** dropped!\n\nUse `/collect {stone_info['name'].lower().replace(' ', '')}` to collect!")
    
    # Add group restriction info if not in the allowed group
    if chat_id and not await is_infinity_stones_allowed_in_chat(chat_id):
        return base + "\n\n⚠️ **Infinity Stones only drop in the designated group!**"
    
    return base + "\n\n🧤 Requires <b>Infinity Gauntlet</b> (50L tokens + 15K shards) — craft with /craftgauntlet"

async def track_infinity_stone_drop(chat_id: int, stone_info: Dict):
    """Track an infinity stone drop in a chat"""
    if chat_id not in recent_infinity_stone_drops:
        recent_infinity_stone_drops[chat_id] = {}
    
    stone_type = stone_info['stone_type']
    recent_infinity_stone_drops[chat_id][stone_type] = datetime.now().timestamp()
    
    # Also track in active drops for compatibility
    active_infinity_stone_drops[chat_id] = {
        'stone_type': stone_type,
        'character_id': stone_info['character_id'],
        'dropped_at': datetime.now(),
        'expires_at': datetime.now().timestamp() + 300  # 5 minutes
    }
    
    print(f"💎 Tracked {stone_type} drop in chat {chat_id}")

async def is_stone_drop_valid(chat_id: int, stone_type: str) -> bool:
    """Check if a stone drop is valid and recent (within 5 minutes)"""
    if chat_id not in recent_infinity_stone_drops:
        return False
    
    if stone_type not in recent_infinity_stone_drops[chat_id]:
        # Fallback: check active_drops from drop module to avoid rare desync
        try:
            from modules.drop import active_drops, drop_expiry_times
            if chat_id in active_drops and active_drops[chat_id]:
                # Check latest relevant drop
                for drop in reversed(active_drops[chat_id]):
                    if drop.get('is_infinity_stone') and drop.get('stone_type') == stone_type:
                        msg_id = drop.get('drop_message_id')
                        expiry_time = drop_expiry_times.get(msg_id)
                        if expiry_time:
                            from datetime import datetime as _dt
                            if _dt.now() < expiry_time:
                                return True
                        break
        except Exception:
            pass
        return False
    
    drop_time = recent_infinity_stone_drops[chat_id][stone_type]
    current_time = datetime.now().timestamp()
    
    # Check if drop is within 5 minutes
    if current_time - drop_time > 300:  # 5 minutes
        # Remove expired drop
        del recent_infinity_stone_drops[chat_id][stone_type]
        return False
    
    return True

async def clear_expired_drops():
    """Clear all expired infinity stone drops"""
    current_time = datetime.now().timestamp()
    expired_chats = []
    
    for chat_id, drops in recent_infinity_stone_drops.items():
        expired_stones = []
        for stone_type, drop_time in drops.items():
            if current_time - drop_time > 300:  # 5 minutes
                expired_stones.append(stone_type)
        
        # Remove expired stones
        for stone_type in expired_stones:
            del drops[stone_type]
        
        # Remove empty chat entries
        if not drops:
            expired_chats.append(chat_id)
    
    # Remove empty chats
    for chat_id in expired_chats:
        del recent_infinity_stone_drops[chat_id]
    
    if expired_chats:
        print(f"🧹 Cleared expired drops from {len(expired_chats)} chats")

async def clear_infinity_stone_drop(chat_id: int):
    """Clear an infinity stone drop from a chat"""
    if chat_id in active_infinity_stone_drops:
        del active_infinity_stone_drops[chat_id]
    
    if chat_id in recent_infinity_stone_drops:
        del recent_infinity_stone_drops[chat_id]

async def get_active_infinity_stone_drop(chat_id: int) -> Optional[Dict]:
    """Get the active infinity stone drop in a chat"""
    return active_infinity_stone_drops.get(chat_id)

async def craft_gauntlet_command(client: Client, message: Message):
    """Handle /craftgauntlet command"""
    user_id = message.from_user.id
    
    # Check if user already has the gauntlet
    if await has_infinity_gauntlet(user_id):
        await message.reply_text("🧤 You already possess the <b>Infinity Gauntlet</b>!")
        return
    
    # Crafting requirements
    required_tokens = 5000000  # 50 lakh tokens
    required_shards = 15000    # 15k shards
    
    # Check if user has enough tokens and shards
    has_enough_tokens, current_tokens = await check_user_tokens(user_id, required_tokens)
    has_enough_shards, current_shards = await check_user_shards(user_id, required_shards)
    
    if not has_enough_tokens:
        await message.reply_text(
            f"❌ <b>Insufficient Tokens!</b>\n\n"
            f"You need <b>{required_tokens:,} tokens</b> to craft the Infinity Gauntlet.\n"
            f"Current balance: <b>{current_tokens:,} tokens</b>"
        )
        return
    
    if not has_enough_shards:
        await message.reply_text(
            f"❌ <b>Insufficient Shards!</b>\n\n"
            f"You need <b>{required_shards:,} shards</b> to craft the Infinity Gauntlet.\n"
            f"Current balance: <b>{current_shards:,} shards</b>"
        )
        return
    
    # Show confirmation dialog
    confirmation_text = (
        f"🧤 **<b>FORGE INFINITY GAUNTLET?</b>** 🧤\n\n"
        f"**Cost:** {required_tokens:,} tokens + {required_shards:,} shards\n\n"
        f"Are you sure you want to forge this infinity gauntlet?\n"
        f"It costs {required_tokens:,} tokens and {required_shards:,} shards.\n\n"
    )
    
    # Create inline keyboard with confirm and cancel buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ CONFIRM", callback_data=f"forge_gauntlet_confirm_{user_id}"),
            InlineKeyboardButton("❌ CANCEL", callback_data=f"forge_gauntlet_cancel_{user_id}")
        ]
    ])
    
    # Send confirmation message with gauntlet image
    gauntlet_image_url = "https://i.ibb.co/fzP8z90P/photo-2025-08-13-20-48-14-Picsart-Ai-Image-Enhancer.jpg"
    
    try:
        await message.reply_photo(
            photo=gauntlet_image_url,
            caption=confirmation_text,
            reply_markup=keyboard
        )
    except Exception as e:
        # Fallback to text if image fails
        await message.reply_text(
            confirmation_text,
            reply_markup=keyboard
        )

async def handle_gauntlet_confirmation(client: Client, callback_query):
    """Handle the confirmation callback for forging the gauntlet"""
    user_id = callback_query.from_user.id
    callback_data = callback_query.data
    
    if callback_data.startswith("forge_gauntlet_cancel_"):
        # User cancelled
        await callback_query.answer("❌ Gauntlet forging cancelled!")
        await callback_query.edit_message_text("❌ **Gauntlet forging cancelled!**\n\nNo resources were spent.")
        return
    
    elif callback_data.startswith("forge_gauntlet_confirm_"):
        # User confirmed - proceed with forging
        await callback_query.answer("🔄 Forging gauntlet...")
        
        # Crafting requirements
        required_tokens = 5000000  # 50 lakh tokens
        required_shards = 15000    # 15k shards
        
        # Double-check if user still has enough resources
        has_enough_tokens, current_tokens = await check_user_tokens(user_id, required_tokens)
        has_enough_shards, current_shards = await check_user_shards(user_id, required_shards)
        
        if not has_enough_tokens or not has_enough_shards:
            await callback_query.edit_message_text(
                "❌ **Insufficient Resources!**\n\n"
                "You no longer have enough tokens or shards to craft the Infinity Gauntlet.\n"
                "Please check your balance and try again."
            )
            return
        
        # Deduct the required resources
        resources_deducted = await deduct_crafting_resources(user_id, required_tokens, required_shards)
        if not resources_deducted:
            await callback_query.edit_message_text("❌ Failed to deduct crafting resources. Please try again later.")
            return
        
        # Attempt to craft the gauntlet
        success = await add_item_to_user(user_id, INFINITY_GAUNTLET_TYPE, INFINITY_GAUNTLET_NAME, "crafted")
        
        if success:
            # Send success message with the gauntlet image
            gauntlet_image_url = "https://i.ibb.co/fzP8z90P/photo-2025-08-13-20-48-14-Picsart-Ai-Image-Enhancer.jpg"
            
            try:
                await callback_query.edit_message_media(
                    media=InputMediaPhoto(
                        media=gauntlet_image_url,
                        caption=(
                            "🧤 **<b>INFINITY GAUNTLET FORGED!</b>** 🧤\n\n"
                            "**Cost:** 50,00,000 tokens + 15,000 shards\n\n"
                            "The legendary gauntlet has been crafted!\n"
                            "You can now collect the Infinity Stones.\n\n"
                        )
                    )
                )
            except Exception as e:
                # Fallback to text if image fails
                await callback_query.edit_message_text(
                    "🧤 **<b>INFINITY GAUNTLET FORGED!</b>** 🧤\n\n"
                    "**Cost:** 50,00,000 tokens + 15,000 shards\n\n"
                    "The legendary gauntlet has been crafted!\n"
                    "You can now collect the Infinity Stones.\n\n"
                    "**Try:** <code>/collect space</code>"
                )
        else:
            await callback_query.edit_message_text("❌ Failed to craft the Infinity Gauntlet. Please try again later.")

async def get_infinity_stones_group_info() -> str:
    """Get information about which group allows infinity stone drops"""
    return (
        f"💎 <b>Infinity Stones Group Restriction</b>\n\n"
        f"<b>Allowed Group ID:</b> <code>{INFINITY_STONES_ALLOWED_GROUP_ID}</code>\n\n"
        f"<b>Note:</b> Infinity Stones will only drop in the designated group.\n"
        f"Other groups will not receive infinity stone drops.\n\n"
        f"<b>To collect Infinity Stones:</b>\n"
        f"1. Join the designated group\n"
        f"2. Craft the Infinity Gauntlet (<code>/craftgauntlet</code>)\n"
        f"3. Wait for drops or use <code>/collect</code> command"
    )

async def check_infinity_stones_group_access(chat_id: int) -> Dict[str, any]:
    """Check if the current chat has access to infinity stone drops"""
    is_allowed = await is_infinity_stones_allowed_in_chat(chat_id)
    
    return {
        "is_allowed": is_allowed,
        "chat_id": chat_id,
        "allowed_group_id": INFINITY_STONES_ALLOWED_GROUP_ID,
        "message": "✅ Infinity Stones are allowed in this group!" if is_allowed else "❌ Infinity Stones are not allowed in this group."
    }

async def infinity_stones_group_info_command(client: Client, message: Message):
    """Handle /infinitygroup command to show group restriction info"""
    await message.reply_text(await get_infinity_stones_group_info())

async def check_group_access_command(client: Client, message: Message):
    """Handle /checkgroup command to check if current group allows infinity stones"""
    chat_id = message.chat.id
    access_info = await check_infinity_stones_group_access(chat_id)
    
    status_emoji = "✅" if access_info["is_allowed"] else "❌"
    
    response_text = (
        f"{status_emoji} <b>Infinity Stones Group Access Check</b>\n\n"
        f"<b>Current Chat ID:</b> <code>{chat_id}</code>\n"
        f"<b>Allowed Group ID:</b> <code>{access_info['allowed_group_id']}</code>\n\n"
        f"<b>Status:</b> {access_info['message']}\n\n"
    )
    
    if not access_info["is_allowed"]:
        response_text += (
            f"<b>To access Infinity Stones:</b>\n"
            f"• Join the designated group\n"
            f"• Use <code>/infinitygroup</code> for more info"
        )
    
    await message.reply_text(response_text)

async def show_active_drops_command(client: Client, message: Message):
    """Handle /activedrops command to show current active infinity stone drops"""
    chat_id = message.chat.id
    
    # Check if user is in the allowed group for infinity stones
    if not await is_infinity_stones_allowed_in_chat(chat_id):
        await message.reply_text(
            "❌ <b>Infinity Stones are not available in this group!</b>\n\n"
            f"Infinity Stones only drop in group <code>{INFINITY_STONES_ALLOWED_GROUP_ID}</code>\n\n"
            "Use <code>/infinitygroup</code> for more information."
        )
        return
    
    # Clear expired drops first
    await clear_expired_drops()
    
    if chat_id not in recent_infinity_stone_drops or not recent_infinity_stone_drops[chat_id]:
        await message.reply_text(
            "💎 <b>No active Infinity Stone drops</b>\n\n"
            "Currently no Infinity Stones are available for collection.\n\n"
            "Drops are very rare and random. Keep checking!"
        )
        return
    
    # Show active drops
    drops_text = "💎 <b>Active Infinity Stone Drops</b>\n\n"
    current_time = datetime.now().timestamp()
    
    for stone_type, drop_time in recent_infinity_stone_drops[chat_id].items():
        stone_info = INFINITY_STONES.get(stone_type, {})
        emoji = stone_info.get('emoji', '💎')
        name = stone_info.get('name', stone_type)
        
        # Calculate time remaining
        time_remaining = 300 - (current_time - drop_time)
        minutes = int(time_remaining // 60)
        seconds = int(time_remaining % 60)
        
        drops_text += f"{emoji} <b>{name}</b>\n"
        drops_text += f"   Time remaining: {minutes}m {seconds}s\n"
        drops_text += f"   Use: <code>/collect {stone_type.replace('_', ' ')}</code>\n\n"
    
    drops_text += "<b>Note:</b> Drops expire after 5 minutes!"
    
    await message.reply_text(drops_text)

# REMOVED: snap pagination function

# Remove all snap-related functions and add supremestore function

async def supremestore_command(client: Client, message: Message):
    """Handle /supremestore command - shows supreme characters for purchase"""
    try:
        # Only allow in private chat
        if message.chat.type != ChatType.PRIVATE:
            await message.reply_text("<b>Please use the /supremestore command in the bot's DM (private chat)</b>")
            return
        
        db = get_database()
        user_id = message.from_user.id
        
        # Check if user has all 6 infinity stones
        collection_status = await get_infinity_stones_collection(user_id)
        owned_count = sum(collection_status.values())
        total_stones = len(INFINITY_STONES)
        
        if owned_count < total_stones:
            missing_stones = []
            for stone_type, is_owned in collection_status.items():
                if not is_owned:
                    stone_info = INFINITY_STONES[stone_type]
                    missing_stones.append(f"{stone_info['emoji']} {stone_info['name']}")
            
            await message.reply_text(
                f"❌ <b>Access Denied!</b> ❌\n\n"
                f"<b>You need all 6 Infinity Stones to access the Supreme Store!</b>\n\n"
                f"<b>Your Progress:</b> {owned_count}/{total_stones} stones\n\n"
                f"<b>Missing Stones:</b>\n" + "\n".join([f"• {stone}" for stone in missing_stones]) + "\n\n"
            )
            return
        
        # User has all stones, proceed with store
        # Get user's supreme store offer
        offer = await get_supreme_store_offer(db, user_id)
        
        # Batch fetch all characters for speed using PostgreSQL
        char_ids = offer["characters"]
        char_docs = await db.get_characters_by_ids(char_ids)
        
        id_to_char = {c.get('character_id') or c.get('id') or c.get('_id'): c for c in char_docs}
        chars = [id_to_char.get(cid) for cid in char_ids if id_to_char.get(cid)]
        
        # Safety check: filter out any characters with is_video=True that might have slipped through
        chars = [c for c in chars if not c.get("is_video", False)]
        
        refreshes = offer.get("refreshes", 0)
        text = format_supreme_store_message(chars, refreshes)
        
        # Only show buttons in private chat
        reply_markup = get_supreme_store_keyboard(refreshes)
        
        await message.reply_photo(
            "https://ibb.co/cKVVS0wm",  # Use same store image
            caption=text,
            reply_markup=reply_markup
        )
        
    except Exception as e:
        print(f"❌ Error in supremestore command: {e}")
        await message.reply_text(
            f"❌ <b>An error occurred while loading the supreme store.</b>\n\n"
            f"Error: {str(e)}\n\n"
            "Please try again later or contact support."
        )

async def get_supreme_store_offer(db, user_id):
    """Get or generate the supreme store offer for a user."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    user = await db.get_user(user_id)
    offer = user.get("supreme_store_offer", {}) if user else {}
    
    # Defensive: If offer is a string, parse it as JSON
    import json
    if isinstance(offer, str):
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}
    
    if not isinstance(offer, dict):
        offer = {}
    
    # Only generate a new store if it's a new day or offer is missing
    if not offer or offer.get("date") != today:
        offer_chars = await get_supreme_characters_for_store(db, 10)
        offer = {
            "date": today,
            "characters": [c.get("character_id") or c.get("id") or c.get("_id") for c in offer_chars],
            "refreshes": 0,
            "purchased": [],
            "pending_buy": None
        }
        await db.update_user(user_id, {"supreme_store_offer": offer})
    
    # Ensure all required fields exist for compatibility
    if "refreshes" not in offer:
        offer["refreshes"] = 0
    if "purchased" not in offer:
        offer["purchased"] = []
    if "pending_buy" not in offer:
        offer["pending_buy"] = None
    if "characters" not in offer or not isinstance(offer["characters"], list) or not offer["characters"]:
        # Generate a new offer if characters is missing or invalid
        offer_chars = await get_supreme_characters_for_store(db, 10)
        offer["characters"] = [c.get("character_id") or c.get("id") or c.get("_id") for c in offer_chars]
        await db.update_user(user_id, {"supreme_store_offer": offer})
    
    return offer

async def get_supreme_characters_for_store(db, count=10):
    """Get supreme characters for the store"""
    try:
        # Get all supreme characters
        supreme_chars = await db.get_all_characters_by_rarity("Supreme")
        
        if not supreme_chars:
            # Fallback: try to get any high rarity characters
            high_rarities = ["Supreme", "Premium", "Zenith", "Ethereal", "Mythic"]
            supreme_chars = []
            for rarity in high_rarities:
                chars = await db.get_all_characters_by_rarity(rarity)
                if chars:
                    supreme_chars.extend(chars)
        
        # Filter out video characters and take random sample
        available_chars = [c for c in supreme_chars if not c.get("is_video", False)]
        
        if len(available_chars) <= count:
            return available_chars
        
        # Return random sample
        import random
        return random.sample(available_chars, count)
        
    except Exception as e:
        print(f"❌ Error getting supreme characters for store: {e}")
        return []

def format_supreme_store_message(chars, refreshes, refresh_cost=1):
    """Format the supreme store message"""
    # Safety check: filter out any characters with is_video=True that might have slipped through
    chars = [c for c in chars if not c.get("is_video", False)]
    
    msg = "<b>🌟 SUPREME STORE - Premium Characters Only 🌟</b>\n\n"
    msg += "💎 <b>REQUIREMENT:</b> You must own all 6 Infinity Stones to access this store!\n\n"
    
    for c in chars:
        emoji = "👑" if c["rarity"] == "Supreme" else "💎"
        # Always prefer character_id for display
        char_id = c.get("character_id") or c.get("id") or c.get("_id")
        msg += f"<b>\n{emoji} {char_id} {c['name']}\nRarity: {c['rarity']} | Price: FREE (0 tokens)\n</b>"
    
    msg += "\n"
    if refreshes == 0:
        msg += f"<b>🔄 1 free refresh left today</b>"
    else:
        left = 1 - refreshes
        if left > 0:
            msg += f"<b>🔄 Refresh cost: {refresh_cost:,} tokens. ({left} left today)</b>"
        else:
            msg += "<b>❌ No refreshes left today.</b>"
    
    return msg

def get_supreme_store_keyboard(refreshes):
    """Get the supreme store keyboard"""
    buttons = []
    # Add buy button
    buttons.append([InlineKeyboardButton("🛒 Buy Character", callback_data="buy_from_supreme_store")])
    
    # Add refresh button if available
    if refreshes < 1:  # Max 10 refreshes for supreme store
        btn_text = "🔄 Refresh Store (Free)" if refreshes == 0 else f"🔄 Refresh Store ({1:,} tokens)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data="refresh_supreme_store")])
    
    return InlineKeyboardMarkup(buttons) if buttons else None

# Supreme store callback handlers
async def buy_from_supreme_store_callback(client: Client, callback_query: CallbackQuery):
    """Handle buy from supreme store callback"""
    # Mark user as waiting for ID input
    waiting_for_supreme_store_id[callback_query.from_user.id] = True
    
    # Ask for character ID
    await callback_query.message.reply(
        "🛒 <b>Buy Character from Supreme Store</b>\n\n"
        "Please enter the ID of the character you want to buy:\n"
        "Example: `123`"
    )
    
    # Delete the store message after sending the prompt
    await callback_query.message.delete()

async def refresh_supreme_store_callback(client: Client, callback_query: CallbackQuery):
    """Handle refresh supreme store callback"""
    db = get_database()
    user_id = callback_query.from_user.id
    today = datetime.utcnow().strftime("%Y-%m-%d")
    user = await db.get_user(user_id)
    offer = user.get("supreme_store_offer", {}) if user else {}
    
    # Defensive: If offer is a string, parse it as JSON
    import json
    if isinstance(offer, str):
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}
    
    refreshes = offer.get("refreshes", 0)
    
    # If offer is not for today, reset refreshes and offer in DB
    if offer.get("date") != today:
        refreshes = 0
        # Reset offer for today, preserving purchased and pending_buy if present
        purchased = offer.get("purchased", [])
        pending_buy = offer.get("pending_buy", None)
        offer["date"] = today
        offer["refreshes"] = 0
        offer["purchased"] = purchased
        offer["pending_buy"] = pending_buy
        await db.update_user(user_id, {"supreme_store_offer": offer})
        # Re-fetch offer from DB to ensure we have the latest state
        user = await db.get_user(user_id)
        offer = user.get("supreme_store_offer", {}) if user else {}
        refreshes = offer.get("refreshes", 0)
    
    if refreshes >= 10:  # Max 10 refreshes for supreme store
        await callback_query.answer("No refreshes left today!", show_alert=True)
        return
    
    free_refresh = refreshes == 0
    # Check tokens if not free
    if not free_refresh and user.get("wallet", 0) < 1:
        await callback_query.answer("Not enough tokens for refresh!", show_alert=True)
        return
    
    # Deduct tokens if not free
    if not free_refresh:
        await db.update_user(user_id, {"wallet": user["wallet"] - 1})
    
    # Generate new offer
    offer_chars = await get_supreme_characters_for_store(db, 10)
    
    new_refreshes = refreshes + 1
    # Preserve purchased and pending_buy fields
    purchased = offer.get("purchased", [])
    pending_buy = offer.get("pending_buy", None)
    new_offer = {
        "date": today,
        "characters": [c.get("character_id") or c.get("id") or c.get("_id") for c in offer_chars],
        "refreshes": new_refreshes,
        "purchased": purchased,
        "pending_buy": pending_buy
    }
    
    await db.update_user(user_id, {"supreme_store_offer": new_offer})
    
    # Batch fetch all characters for speed using PostgreSQL
    char_ids = new_offer["characters"]
    char_docs = await db.get_characters_by_ids(char_ids)
    
    id_to_char = {c.get('character_id') or c.get('id') or c.get('_id'): c for c in char_docs}
    chars = [id_to_char.get(cid) for cid in char_ids if id_to_char.get(cid)]
    
    # Safety check: filter out any characters with is_video=True that might have slipped through
    chars = [c for c in chars if not c.get("is_video", False)]
    
    text = format_supreme_store_message(chars, new_refreshes)
    reply_markup = get_supreme_store_keyboard(new_refreshes)
    
    await callback_query.edit_message_caption(
        caption=text,
        reply_markup=reply_markup
    )
    
    await callback_query.answer("Supreme store refreshed!")

# Dictionary to track users waiting for character ID input (for supreme store)
waiting_for_supreme_store_id = {}

async def process_supreme_store_buy_request(client: Client, message: Message, char_id: int):
    """Process buy request from supreme store"""
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    offer = user.get("supreme_store_offer", {}) if user else {}
    
    # Defensive: If offer is a string, parse it as JSON
    import json
    if isinstance(offer, str):
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}
    
    if not isinstance(offer, dict):
        offer = {}
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Reset purchased list if new day
    if offer.get("date") != today:
        offer["purchased"] = []
        offer["date"] = today
        offer["pending_buy"] = None
        await db.update_user(user_id, {"supreme_store_offer": offer})
    
    purchased = offer.get("purchased", [])
    offer_chars = offer.get("characters", [])
    pending_buy = offer.get("pending_buy")
    
    if char_id not in offer_chars:
        await message.reply_text("This character is not available in your supreme store offer.")
        return
    
    if char_id in purchased:
        await message.reply_text("You have already purchased this character from your supreme store today. You can buy it again tomorrow!")
        return
    
    if pending_buy == char_id:
        await message.reply_text("You already have a pending confirmation for this character. Please confirm or wait before trying again.")
        return
    
    char = await db.get_character(char_id)
    if not char:
        await message.reply_text("Character not found.")
        return
    
    # Set pending confirmation
    offer["pending_buy"] = char_id
    await db.update_user(user_id, {"supreme_store_offer": offer})
    
    # Show confirmation with image and button
    img_url = char.get("img_url")
    caption = (
        f"<b>Confirm Purchase from Supreme Store</b>\n\n"
        f"<b>Name:</b> {char['name']}\n"
        f"<b>Rarity:</b> {char['rarity']}\n"
        f"<b>Price:</b> FREE (0 tokens)\n\n"
        f"Are you sure you want to claim this character?"
    )
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm Purchase", callback_data=f"confirm_supreme_buy_{char_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_supreme_buy_{char_id}")
        ]
    ])
    
    if img_url:
        await message.reply_photo(img_url, caption=caption, reply_markup=keyboard)
    else:
        await message.reply_text(caption, reply_markup=keyboard)

async def confirm_supreme_buy_callback(client: Client, callback_query: CallbackQuery):
    """Handle confirm supreme buy callback"""
    db = get_database()
    user_id = callback_query.from_user.id
    char_id = int(callback_query.data.split("_")[-1])
    user = await db.get_user(user_id)
    offer = user.get("supreme_store_offer", {}) if user else {}
    
    # Defensive: If offer is a string, parse it as JSON
    import json
    if isinstance(offer, str):
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}
    
    if not isinstance(offer, dict):
        offer = {}
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Reset purchased list if new day
    if offer.get("date") != today:
        offer["purchased"] = []
        offer["date"] = today
        offer["pending_buy"] = None
        await db.update_user(user_id, {"supreme_store_offer": offer})
    
    purchased = offer.get("purchased", [])
    offer_chars = offer.get("characters", [])
    pending_buy = offer.get("pending_buy")
    
    if char_id not in offer_chars:
        await callback_query.answer("Not in your supreme store offer.", show_alert=True)
        return
    
    if char_id in purchased:
        await callback_query.answer("Already purchased today.", show_alert=True)
        return
    
    if pending_buy != char_id:
        await callback_query.answer("No pending confirmation for this character. Please use /buy <id> again.", show_alert=True)
        return
    
    char = await db.get_character(char_id)
    if not char:
        await callback_query.answer("Character not found.", show_alert=True)
        return
    
    # Add character, mark as purchased, clear pending (no token deduction)
    await db.update_user(user_id, {
        "characters": user.get("characters", []) + [char_id],
        "supreme_store_offer": {
            **offer,
            "purchased": purchased + [char_id],
            "pending_buy": None,
            "date": today
        }
    })
    
    # Remove all infinity stones from user's inventory after purchase
    pool = get_postgres_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM user_items 
                WHERE user_id = $1 AND item_type LIKE '%_stone'
            """, user_id)
        print(f"🗑️ Removed all infinity stones from user {user_id} after supreme character purchase")
    except Exception as e:
        print(f"❌ Error removing infinity stones: {e}")
    
    # Log transaction
    await db.log_user_transaction(user_id, "supreme_store_purchase", {
        "character_id": char_id,
        "name": char["name"],
        "rarity": char["rarity"],
        "price": 0,
        "date": today,
        "infinity_stones_consumed": True
    })
    
    await callback_query.edit_message_caption(
        caption=f"<b>Congratulations!</b> You claimed <b>{char['name']}</b> for FREE from the Supreme Store!\n\n"
                f"💎 <b>All 6 Infinity Stones have been consumed!</b>\n"
                f"You'll need to collect them again to access the Supreme Store once more."
    )
    
    await callback_query.answer("Purchase successful!")

async def cancel_supreme_buy_callback(client: Client, callback_query: CallbackQuery):
    """Handle cancel supreme buy callback"""
    db = get_database()
    user_id = callback_query.from_user.id
    char_id = int(callback_query.data.split("_")[-1])
    user = await db.get_user(user_id)
    offer = user.get("supreme_store_offer", {}) if user else {}
    
    # Defensive: If offer is a string, parse it as JSON
    import json
    if isinstance(offer, str):
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}
    
    if not isinstance(offer, dict):
        offer = {}
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Reset purchased list if new day
    if offer.get("date") != today:
        offer["purchased"] = []
        offer["date"] = today
        offer["pending_buy"] = None
        await db.update_user(user_id, {"supreme_store_offer": offer})
    
    pending_buy = offer.get("pending_buy")
    if not pending_buy:
        await callback_query.edit_message_caption(
            caption="<b>No purchase is currently pending.</b>"
        )
        await callback_query.answer("No purchase is currently pending.")
        return
    
    # Always clear pending buy, regardless of char_id
    offer["pending_buy"] = None
    await db.update_user(user_id, {"supreme_store_offer": offer})
    
    await callback_query.edit_message_caption(
        caption="<b>Purchase cancelled.</b>"
    )
    
    await callback_query.answer("Purchase cancelled.")

# Add supremestore command to the module exports
__all__ = [
    'create_items_table',
    'get_user_items',
    'add_item_to_user',
    'check_item_ownership',
    'has_infinity_gauntlet',
    'check_user_tokens',
    'check_user_shards',
    'deduct_crafting_resources',
    'get_infinity_stones_collection',
    'attempt_stone_collection',
    'attempt_infinity_stone_drop_collection',
    'attempt_stone_collection_by_short_name',
    'get_random_infinity_stone_for_drop',
    'is_infinity_stone_character',
    'get_infinity_stone_info',
    'inventory_command',
    'collect_stone_command',
    'handle_infinity_stones_callback',
    'get_infinity_stones_stats',
    'initialize_infinity_stones',
    'is_infinity_stones_allowed_in_chat',
    'should_drop_infinity_stone',
    'create_infinity_stone_drop_message',
    'track_infinity_stone_drop',
    'is_stone_drop_valid',
    'clear_expired_drops',
    'clear_infinity_stone_drop',
    'get_active_infinity_stone_drop',
    'craft_gauntlet_command',
    'handle_gauntlet_confirmation',
    'get_infinity_stones_group_info',
    'check_infinity_stones_group_access',
    'infinity_stones_group_info_command',
    'check_group_access_command',
    'show_active_drops_command',
    'supremestore_command',
    'get_supreme_store_offer',
    'get_supreme_characters_for_store',
    'format_supreme_store_message',
    'get_supreme_store_keyboard',
    'buy_from_supreme_store_callback',
    'refresh_supreme_store_callback',
    'process_supreme_store_buy_request',
    'confirm_supreme_buy_callback',
    'cancel_supreme_buy_callback'
]
