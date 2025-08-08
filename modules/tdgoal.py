from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from .decorators import check_banned
import os
from modules.postgres_database import get_database


from datetime import datetime

# Only two daily collection tasks (no test)
TASKS = [
    {
        "id": "collect_100_today",
        "name": "Collect 100 Characters Today",
        "required": 100,
        "reward": 90000
    },
    {
        "id": "collect_300_today",
        "name": "Collect 300 Characters Today",
        "required": 300,
        "reward": 350000
    },
    {
        "id": "collect_500_today",
        "name": "Collect 500 Characters Today",
        "required": 500,
        "reward": 600000,
        "extra_reward": True
    },
    {
        "id": "collect_800_today",
        "name": "Collect 800 Characters Today",
        "required": 800,
        "reward": 0,
        "extra_reward": "ultimate_2"
    },
    {
        "id": "collect_1_today",
        "name": "Collect 1 Character Today",
        "required": 1,
        "reward": 1000
    }
]

async def tdgoal_command(client: Client, message_or_user, edit_message=None):
    if isinstance(message_or_user, int):
        user_id = message_or_user
        db = get_database()
        user = await db.get_user(user_id)
        if not user:
            if edit_message:
                await edit_message.edit_text("❌ User not found!")
            return
    else:
        user_id = message_or_user.from_user.id
        db = get_database()
        user = await db.get_user(user_id)
        if not user:
            await message_or_user.reply_text("❌ User not found!")
            return
    today = datetime.now().date().isoformat()
    import json

    progress = user.get('tdgoal_progress')
    if not progress:
        progress = {}
    elif isinstance(progress, str):
        try:
            progress = json.loads(progress)
        except Exception:
            progress = {}
    today_progress = progress.get(today, 0)
    if isinstance(today_progress, str):
        try:
            today_progress = int(today_progress)
        except Exception:
            today_progress = 0

    claimed = user.get('tdgoal_claimed')
    if not claimed:
        claimed = {}
    elif isinstance(claimed, str):
        try:
            claimed = json.loads(claimed)
        except Exception:
            claimed = {}
    claimed_today = claimed.get(today, [])
    buttons = []
    for task in TASKS:
        is_claimed = task['id'] in claimed_today
        if today_progress >= task['required']:
            if is_claimed:
                btn = InlineKeyboardButton(f"✅ {task['name']} (Claimed)", callback_data="tdgoal_done")
            else:
                btn = InlineKeyboardButton(f"🎁 {task['name']} (+{task['reward']:,} tokens)", callback_data=f"tdgoal_claim_{task['id']}")
        else:
            btn = InlineKeyboardButton(f"📋 {task['name']} ({today_progress}/{task['required']}) (+{task['reward']:,} tokens)", callback_data="tdgoal_locked")
        buttons.append([btn])
    keyboard = InlineKeyboardMarkup(buttons)
    text = (
        f"<b>🔥 Daily Collection Tasks</b>\n\n"
        f"Collect characters today to earn big token rewards!\n\n"
        f"<code>Progress: {today_progress} collected today</code>\n\n"
        f"<b>Available Rewards:</b>\n"
        f"• 100 characters = <code>90,000 tokens</code>\n"
        f"• 300 characters = <code>350,000 tokens</code>\n"
        f"• 500 characters = <code>600,000 tokens</code> + <b>random Mythic/Zenith/Ethereal character</b>"
        f"• 800 characters =  <b>2 random Ultimate characters</b>"
        f"• 1 character = <code>1,000 tokens</code>\n\n"
    )
    if edit_message:
        await edit_message.edit_text(text, reply_markup=keyboard)
    else:
        await message_or_user.reply_text(text, reply_markup=keyboard)

async def tdgoal_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    db = get_database()
    # Restrict access to the user who initiated the command
    if callback_query.message.reply_to_message:
        initiator_id = callback_query.message.reply_to_message.from_user.id
        if user_id != initiator_id:
            await callback_query.answer("Access denied: not your task menu!", show_alert=True)
            return
    data = callback_query.data
    if data == "tdgoal_done":
        await callback_query.answer("Already claimed!", show_alert=True)
        return
    if data == "tdgoal_locked":
        await callback_query.answer("Keep collecting to unlock this reward!", show_alert=True)
        return
    if data.startswith("tdgoal_claim_"):
        task_id = data.replace("tdgoal_claim_", "")
        today = datetime.now().date().isoformat()
        try:
            # Only fetch needed fields
            user = await db.users.find_one({"user_id": user_id}, {"user_id": 1, "tdgoal_progress": 1, "tdgoal_claimed": 1, "wallet": 1, "characters": 1})
        except Exception as e:
            await callback_query.answer("Database error!", show_alert=True)
            print(f"tdgoal_callback DB error: {e}")
            return
        if not user:
            await callback_query.answer("User not found!", show_alert=True)
            return
        progress = user.get('tdgoal_progress', {})
        # Ensure progress is a dict, not a string
        if not progress:
            progress = {}
        elif isinstance(progress, str):
            try:
                import json
                progress = json.loads(progress)
            except Exception:
                progress = {}
        today_progress = progress.get(today, 0)
        if isinstance(today_progress, str):
            try:
                today_progress = int(today_progress)
            except Exception:
                today_progress = 0
        claimed = user.get('tdgoal_claimed')
        if not claimed:
            claimed = {}
        elif isinstance(claimed, str):
            try:
                import json
                claimed = json.loads(claimed)
            except Exception:
                claimed = {}
        claimed_today = claimed.get(today, [])
        task = next((t for t in TASKS if t['id'] == task_id), None)
        if not task:
            await callback_query.answer("Task not found!", show_alert=True)
            return
        if task_id in claimed_today:
            await callback_query.answer("Already claimed!", show_alert=True)
            return
        if today_progress < task['required']:
            await callback_query.answer("Not enough collections today!", show_alert=True)
            return
        # Give reward and mark as claimed
        update_ops = {
            '$push': {f'tdgoal_claimed.{today}': task_id}
        }
        reward_msg = f"<b>🎉 Reward Claimed!</b>\n\n"
        got_reward = False
        if task['reward'] > 0:
            update_ops['$inc'] = {'wallet': task['reward']}
            reward_msg += f"You received <b>{task['reward']:,} tokens</b>!\n"
            got_reward = True
        extra_msg = ""
        # Extra reward for 500 task: random character of Mythic, Zenith, or Ethereal
        if task.get("extra_reward") == True:
            rarities = ["Mythic", "Zenith", "Ethereal"]
            try:
                chars = await db.get_random_character_by_rarities(rarities)
            except Exception as e:
                print(f"tdgoal_callback get_random_character_by_rarities error: {e}")
                chars = []
            if chars:
                char_id = chars.get("id") or chars.get("character_id") or chars.get("_id")
                try:
                    await db.users.update_one(
                        {'user_id': user_id},
                        {'$push': {'characters': char_id}}
                    )
                    extra_msg = f"You also received a random <b>{chars['rarity']}</b> character: <b>{chars['name']}</b>!\n"
                    got_reward = True
                except Exception as e:
                    print(f"tdgoal_callback error updating user with extra char: {e}")
            else:
                extra_msg = "No eligible characters available for extra reward.\n"
        # Extra reward for 800 task: 2 Ultimate characters
        elif task.get("extra_reward") == "ultimate_2":
            try:
                # Get up to 2 random Ultimate characters, excluding IDs 926 and 940
                exclude_ids = [926, 940]
                selected = []
                
                # Directly query for Ultimate characters using aggregation
                cursor = db.characters.aggregate([
                    {'$match': {'rarity': 'Ultimate'}},
                    {'$sample': {'size': 22}}  # Get a pool of 22 Ultimate characters
                ])
                ultimates = [doc async for doc in cursor]
                
                # Filter out excluded IDs
                ultimate_chars = [c for c in ultimates if 
                                (str(c.get("id")) not in [str(eid) for eid in exclude_ids]) and 
                                (str(c.get("character_id")) not in [str(eid) for eid in exclude_ids]) and 
                                (str(c.get("_id")) not in [str(eid) for eid in exclude_ids])]
                
                import random
                if len(ultimate_chars) >= 2:
                    selected = random.sample(ultimate_chars, 2)
                elif len(ultimate_chars) == 1:
                    selected = ultimate_chars
                else:
                    # Fallback: try to get any Ultimate characters without ID restrictions
                    fallback_cursor = db.characters.aggregate([
                        {'$match': {'rarity': 'Ultimate'}},
                        {'$sample': {'size': 5}}
                    ])
                    fallback_ultimates = [doc async for doc in fallback_cursor]
                    if len(fallback_ultimates) >= 2:
                        selected = random.sample(fallback_ultimates, 2)
                    elif len(fallback_ultimates) == 1:
                        selected = fallback_ultimates
            except Exception as e:
                print(f"tdgoal_callback get_random_character_by_rarities_excluding error: {e}")
                selected = []
            char_names = []
            if selected:
                for char in selected:
                    char_id = char.get("id") or char.get("character_id") or char.get("_id")
                    try:
                        await db.users.update_one(
                            {'user_id': user_id},
                            {'$push': {'characters': char_id}}
                        )
                        char_names.append(f"<b>{char['name']}</b>")
                        got_reward = True
                    except Exception as e:
                        print(f"tdgoal_callback error updating user with ultimate char: {e}")
                if char_names:
                    extra_msg = f"You also received {len(char_names)} random <b>Ultimate</b> character{'s' if len(char_names) > 1 else ''}: {', '.join(char_names)}!\n"
                else:
                    extra_msg = "No Ultimate characters available for extra reward.\n"
            else:
                extra_msg = "No Ultimate characters available for extra reward.\n"
        reward_msg += extra_msg
        if not got_reward:
            reward_msg += "No rewards could be given at this time. Please contact support."
        try:
            await db.users.update_one(
                {'user_id': user_id},
                update_ops
            )
        except Exception as e:
            print(f"tdgoal_callback error updating claimed status: {e}")
            await callback_query.answer("Database error updating claim!", show_alert=True)
            return
        try:
            await callback_query.message.edit_text(reward_msg)
        except Exception as e:
            print(f"tdgoal_callback error editing message: {e}")
        try:
            await callback_query.answer("Reward claimed!", show_alert=True)
        except Exception as e:
            print(f"tdgoal_callback error answering callback: {e}")
        return

# Call this from collect_handler in main.py after a successful collect
async def track_collect_drop(user_id: int):
    try:
        db = get_database()
        today = datetime.now().date().isoformat()
        user = await db.get_user(user_id)
        if not user:
            return
        import json
        progress = user.get('tdgoal_progress')
        if not progress:
            progress = {}
        elif isinstance(progress, str):
            try:
                progress = json.loads(progress)
            except Exception:
                progress = {}
        today_val = progress.get(today, 0)
        if isinstance(today_val, str):
            try:
                today_val = int(today_val)
            except Exception:
                today_val = 0
        today_progress = today_val + 1
        await db.users.update_one(
            {'user_id': user_id},
            {'$set': {f'tdgoal_progress.{today}': today_progress}}
        )
    except Exception as e:
        print(f"tdgoal track_collect_drop error: {e}")