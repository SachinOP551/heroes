from datetime import datetime, timedelta
from pyrogram.types import Message
from pyrogram import Client
from modules.postgres_database import get_database
from .logging_utils import send_token_log
from .decorators import is_owner, is_og, is_sudo, check_banned
from pyrogram import filters
import random
import asyncio

from pyrogram.enums import ChatType

# BALANCE
@check_banned
async def balance_command(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    wallet = user.get('wallet', 0)
    bank = user.get('bank', 0)
    shards = user.get('shards', 0)
    await message.reply_text(
        f"💸 <b>Your current balance:</b>\n\n"
        f"• <b>Wallet:</b> <code>{wallet:,}</code>\n"
        f"• 🏦 <b>Bank Balance:</b> <code>{bank:,}</code>\n"
        f"• 🎐 <b>Shards:</b> <code>{shards:,}</code>"
    )

# DEPOSIT
@check_banned
async def deposit_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in financial_locks:
        financial_locks[user_id] = asyncio.Lock()
    
    async with financial_locks[user_id]:
        db = get_database()
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ <b>Please specify an amount to deposit!</b>")
            return
        try:
            amount = int(args[1])
            if amount <= 0:
                raise ValueError
            user = await db.get_user(user_id)
            wallet = user.get('wallet', 0)
            if wallet is None:
                wallet = 0
            if wallet < amount:
                await message.reply_text("❌ <b>You don't have enough tokens in your wallet!</b>")
                return
            bank = user.get('bank', 0)
            if bank is None:
                bank = 0
            await db.update_user(user_id, {'wallet': wallet - amount, 'bank': bank + amount})
            await message.reply_text(f"✅ <b>Successfully deposited</b> <code>{amount:,}</code> <b>tokens to your bank!</b>")
        except ValueError:
            await message.reply_text("❌ <b>Please provide a valid positive number!</b>")

# WITHDRAW
@check_banned
async def withdraw_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in financial_locks:
        financial_locks[user_id] = asyncio.Lock()
    
    async with financial_locks[user_id]:
        db = get_database()
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ <b>Please specify an amount to withdraw!</b>")
            return
        try:
            amount = int(args[1])
            if amount <= 0:
                raise ValueError
            user = await db.get_user(user_id)
            wallet = user.get('wallet', 0)
            if wallet is None:
                wallet = 0
            bank = user.get('bank', 0)
            if bank is None:
                bank = 0
            if bank < amount:
                await message.reply_text("❌ <b>You don't have enough tokens in your bank!</b>")
                return
            await db.update_user(user_id, {'wallet': wallet + amount, 'bank': bank - amount})
            await message.reply_text(f"✅ <b>Successfully withdrew</b> <code>{amount:,}</code> <b>tokens from your bank!</b>")
            return
        except ValueError:
            await message.reply_text("❌ <b>Please provide a valid positive number!</b>")

# DAILY
@check_banned
async def daily_command(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    now = datetime.utcnow()
    last_daily = user.get('last_daily')
    if last_daily:
        if isinstance(last_daily, str):
            last_daily_dt = datetime.fromisoformat(last_daily)
        else:
            last_daily_dt = last_daily
        next_daily = last_daily_dt + timedelta(days=1)
        if now < next_daily:
            time_left = next_daily - now
            hours = time_left.seconds // 3600
            minutes = (time_left.seconds % 3600) // 60
            await message.reply_text(
                f"❌ <b>You've already claimed your daily reward!</b>\nPlease wait: <b>{hours}h {minutes}m</b>"
            )
            return
    reward = 5000
    wallet = user.get('wallet', 0)
    await db.update_user(user_id, {'wallet': wallet + reward, 'last_daily': now})
    await message.reply_text(f"✅ <b>Daily reward claimed!</b>\n\n💰 You received: <b>{reward:,}</b> tokens")

# WEEKLY
@check_banned
async def weekly_command(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    now = datetime.utcnow()
    last_weekly = user.get('last_weekly')
    if last_weekly:
        if isinstance(last_weekly, str):
            last_weekly_dt = datetime.fromisoformat(last_weekly)
        else:
            last_weekly_dt = last_weekly
        next_weekly = last_weekly_dt + timedelta(days=7)
        if now < next_weekly:
            time_left = next_weekly - now
            days = time_left.days
            hours = time_left.seconds // 3600
            await message.reply_text(
                f"❌ <b>You've already claimed your weekly reward!</b>\nPlease wait: <b>{days}d {hours}h</b>"
            )
            return
    reward = 15000
    wallet = user.get('wallet', 0)
    await db.update_user(user_id, {'wallet': wallet + reward, 'last_weekly': now})
    await message.reply_text(f"✅ <b>Weekly reward claimed!</b>\n\n💰 You received: <b>{reward:,}</b> tokens")

# MONTHLY
@check_banned
async def monthly_command(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    now = datetime.utcnow()
    last_monthly = user.get('last_monthly')
    if last_monthly:
        if isinstance(last_monthly, str):
            last_monthly_dt = datetime.fromisoformat(last_monthly)
        else:
            last_monthly_dt = last_monthly
        next_monthly = last_monthly_dt + timedelta(days=30)
        if now < next_monthly:
            time_left = next_monthly - now
            days = time_left.days
            hours = time_left.seconds // 3600
            await message.reply_text(
                f"❌ <b>You've already claimed your monthly reward!</b>\nPlease wait: <b>{days}d {hours}h</b>"
            )
            return
    reward = 35000
    wallet = user.get('wallet', 0)
    await db.update_user(user_id, {'wallet': wallet + reward, 'last_monthly': now})
    await message.reply_text(f"✅ <b>Monthly reward claimed!</b>\n\n💰 You received: <b>{reward:,}</b> tokens")

# GIVE TOKENS (ADMIN, REPLY)
@check_banned
async def give_tokens(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    # Check admin
    if not (is_owner(user_id) or user.get('og', False) or user.get('sudo', False)):
        await message.reply_text("❌ <b>This command is restricted to admins only!</b>")
        return
    if not message.reply_to_message:
        await message.reply_text("❌ <b>Please reply to a user's message!</b>")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ <b>Please specify the amount of tokens!</b>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
        target_user = message.reply_to_message.from_user
        target_data = await db.get_user(target_user.id)
        if not target_data:
            await db.add_user({
                'user_id': target_user.id,
                'username': target_user.username,
                'first_name': target_user.first_name,
                'wallet': amount,
                'bank': 0,
                'characters': [],
                'last_daily': None,
                'last_weekly': None,
                'last_monthly': None,
                'sudo': False,
                'og': False,
                'collection_preferences': {'mode': 'default', 'filter': None}
            })
        else:
            new_balance = target_data.get('wallet', 0) + amount
            await db.update_user(target_user.id, {'wallet': new_balance})
        await message.reply_text(f"✅ <b>Successfully gave</b> <code>{amount:,}</code> <b>tokens to</b> {target_user.mention}")
        await send_token_log(client, message.from_user, target_user, amount, action="give_tokens (gbheek)")
    except ValueError:
        await message.reply_text("❌ <b>Please provide a valid positive number!</b>")

# TAKE TOKENS (ADMIN, REPLY)
@check_banned
async def take_tokens(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    if not (is_owner(user_id) or user.get('og', False) or user.get('sudo', False)):
        await message.reply_text("❌ <b>This command is restricted to admins only!</b>")
        return
    if not message.reply_to_message:
        await message.reply_text("❌ <b>Please reply to a user's message!</b>")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ <b>Please specify the amount of tokens!</b>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
        target_user = message.reply_to_message.from_user
        target_data = await db.get_user(target_user.id)
        if not target_data:
            await message.reply_text("❌ <b>User not found!</b>")
            return
        wallet = target_data.get('wallet', 0)
        if wallet < amount:
            await message.reply_text("❌ <b>User doesn't have enough tokens!</b>")
            return
        new_balance = wallet - amount
        await db.update_user(target_user.id, {'wallet': new_balance})
        await message.reply_text(f"✅ <b>Successfully taken</b> <code>{amount:,}</code> <b>tokens from</b> {target_user.mention}")
        await send_token_log(client, message.from_user, target_user, amount, action="take_tokens (gbheek)")
    except ValueError:
        await message.reply_text("❌ <b>Please provide a valid positive number!</b>")

# PAY TOKENS (REPLY)
@check_banned
async def pay_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in financial_locks:
        financial_locks[user_id] = asyncio.Lock()
    
    async with financial_locks[user_id]:
        now = datetime.utcnow()
        last_used = pay_last_used.get(user_id)
        if last_used and (now - last_used).total_seconds() < PAY_COOLDOWN:
            seconds_left = PAY_COOLDOWN - int((now - last_used).total_seconds())
            await message.reply_text(f"⏳ Please wait {seconds_left} seconds before making another payment.")
            return
        
        pay_last_used[user_id] = now
        
        db = get_database()
        if not message.reply_to_message:
            await message.reply_text("❌ <b>Please reply to a user's message!</b>")
            return
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ <b>Please specify the amount of tokens!</b>")
            return
        sender = message.from_user
        receiver = message.reply_to_message.from_user
        if sender.id == receiver.id:
            await message.reply_text("❌ <b>You cannot pay tokens to yourself!</b>")
            return
        if receiver.is_bot:
            await message.reply_text("❌ <b>You cannot pay tokens to the bot!</b>")
            return
        try:
            amount = int(args[1])
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ <b>Please provide a valid positive number!</b>")
            return
        sender_data = await db.get_user(sender.id)
        if not sender_data:
            await message.reply_text("❌ <b>You don't have an account!</b>")
            return
        sender_wallet = sender_data.get('wallet', 0)
        if sender_wallet < amount:
            await message.reply_text("❌ <b>You don't have enough tokens!</b>")
            return
        receiver_data = await db.get_user(receiver.id)
        if not receiver_data:
            await db.add_user({
                'user_id': receiver.id,
                'username': receiver.username,
                'first_name': receiver.first_name,
                'wallet': amount,
                'bank': 0,
                'characters': [],
                'groups': []
            })
        else:
            receiver_wallet = receiver_data.get('wallet', 0)
            await db.update_user(receiver.id, {'wallet': receiver_wallet + amount})
        await db.update_user(sender.id, {'wallet': sender_wallet - amount})
        await message.reply_text(f"✅ <b>Payment successful!</b>\n\n💰 <b>{amount:,}</b> tokens paid to {receiver.mention}")

        # Log transaction for both sender and receiver
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        await db.log_user_transaction(sender.id, "pay_sent", {
            "to_user_id": receiver.id,
            "to_user_name": receiver.first_name,
            "amount": amount,
            "date": now
        })
        await db.log_user_transaction(receiver.id, "pay_received", {
            "from_user_id": sender.id,
            "from_user_name": sender.first_name,
            "amount": amount,
            "date": now
        })

@check_banned
async def shards_pay(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    # Only allow /spay when replying to a user
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("❌ <b>You must reply to a user's message to pay shards!</b>")
        return
    receiver = message.reply_to_message.from_user
    if receiver.is_bot or receiver.id == user_id:
        await message.reply_text("❌ <b>Invalid target user!</b>")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ <b>Please specify the amount of shards!</b>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.reply_text("❌ <b>Please provide a valid positive number!</b>")
        return
    sender_data = await db.get_user(user_id)
    if not sender_data or sender_data.get('shards', 0) < amount:
        await message.reply_text("❌ <b>You don't have enough 🎐 shards!</b>")
        return
    receiver_data = await db.get_user(receiver.id)
    if not receiver_data:
        await db.add_user({
            'user_id': receiver.id,
            'username': receiver.username,
            'first_name': receiver.first_name,
            'wallet': 0,
            'bank': 0,
            'shards': amount,
            'characters': [],
            'groups': []
        })
    else:
        receiver_shards = receiver_data.get('shards', 0)
        await db.update_user(receiver.id, {'shards': receiver_shards + amount})
    sender_shards = sender_data.get('shards', 0)
    await db.update_user(user_id, {'shards': sender_shards - amount})
    await message.reply_text(f"✅ <b>Shards payment successful!</b>\n\n🎐 <b>{amount:,}</b> shards paid to {receiver.mention}")

# GIVE SHARDS (ADMIN, REPLY)
@check_banned
async def give_shards(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    # Check admin
    if not (is_owner(user_id) or user.get('og', False) or user.get('sudo', False)):
        await message.reply_text("❌ <b>This command is restricted to admins only!</b>")
        return
    if not message.reply_to_message:
        await message.reply_text("❌ <b>Please reply to a user's message!</b>")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ <b>Please specify the amount of shards!</b>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
        target_user = message.reply_to_message.from_user
        target_data = await db.get_user(target_user.id)
        if not target_data:
            await db.add_user({
                'user_id': target_user.id,
                'username': target_user.username,
                'first_name': target_user.first_name,
                'wallet': 0,
                'bank': 0,
                'shards': amount,
                'characters': [],
                'last_daily': None,
                'last_weekly': None,
                'last_monthly': None,
                'sudo': False,
                'og': False,
                'collection_preferences': {'mode': 'default', 'filter': None}
            })
        else:
            new_balance = target_data.get('shards', 0) + amount
            await db.update_user(target_user.id, {'shards': new_balance})
        await message.reply_text(f"✅ <b>Successfully gave</b> <code>{amount:,}</code> <b>🎐 shards to</b> {target_user.mention}")
    except ValueError:
        await message.reply_text("❌ <b>Please provide a valid positive number!</b>")

# TAKE SHARDS (ADMIN, REPLY)
@check_banned
async def take_shards(client: Client, message: Message):
    db = get_database()
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    if not (is_owner(user_id) or user.get('og', False) or user.get('sudo', False)):
        await message.reply_text("❌ <b>This command is restricted to admins only!</b>")
        return
    if not message.reply_to_message:
        await message.reply_text("❌ <b>Please reply to a user's message!</b>")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ <b>Please specify the amount of shards!</b>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
        target_user = message.reply_to_message.from_user
        target_data = await db.get_user(target_user.id)
        if not target_data:
            await message.reply_text("❌ <b>User not found!</b>")
            return
        shards = target_data.get('shards', 0)
        if shards < amount:
            await message.reply_text("❌ <b>User doesn't have enough 🎐 shards!</b>")
            return
        new_balance = shards - amount
        await db.update_user(target_user.id, {'shards': new_balance})
        await message.reply_text(f"✅ <b>Successfully taken</b> <code>{amount:,}</code> <b>🎐 shards from</b> {target_user.mention}")
    except ValueError:
        await message.reply_text("❌ <b>Please provide a valid positive number!</b>")



# Cooldown and lock dictionaries for each game command
football_locks = {}
football_last_used = {}
dart_locks = {}
dart_last_used = {}
basket_locks = {}
basket_last_used = {}
roll_locks = {}
roll_last_used = {}

# Payment locks to prevent farming
pay_locks = {}
pay_last_used = {}
sspay_locks = {}
sspay_last_used = {}

# Financial operation locks to prevent race conditions
financial_locks = {}

COOLDOWN_MIN = 120
COOLDOWN_MAX = 180
football_cooldowns = {}
dart_cooldowns = {}
basket_cooldowns = {}
roll_cooldowns = {}

# Payment cooldown (30 seconds between payments)
PAY_COOLDOWN = 30

@check_banned
async def football_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in football_locks:
        football_locks[user_id] = asyncio.Lock()
    async with football_locks[user_id]:
        now = datetime.utcnow()
        last_used = football_last_used.get(user_id)
        cooldown = football_cooldowns.get(user_id, COOLDOWN_MIN)
        if last_used and (now - last_used).total_seconds() < cooldown:
            seconds_left = cooldown - int((now - last_used).total_seconds())
            await message.reply_text(f"⏳ Please wait {seconds_left} seconds before using this command again.")
            return
        football_last_used[user_id] = now
        football_cooldowns[user_id] = random.randint(COOLDOWN_MIN, COOLDOWN_MAX)
        try:
            if message.from_user is None or message.from_user.is_bot:
                await message.reply_text("❌ <b>Only real users can play this game!</b>")
                return
            dice = await client.send_dice(
                chat_id=message.chat.id,
                emoji="⚽"
            )
            if not hasattr(dice, 'dice') or dice.dice is None:
                await message.reply_text("❌ <b>Failed to send football. Please try again later.</b>")
                return
            await asyncio.sleep(3)
            # Score is a goal if value is 4 or 5
            if dice.dice.value in (4, 5):
                reward = random.randint(100, 600)
                db = get_database()
                user = await db.get_user(user_id)
                if not user:
                    await message.reply_text("❌ <b>You need an account to receive rewards. Use /start first!</b>")
                    return
                shards = user.get('shards', 0)
                await db.update_user(user_id, {'shards': shards + reward})
                await db.log_user_transaction(user_id, "football_win", {
                    "reward": reward,
                    "chat_id": message.chat.id,
                    "date": datetime.utcnow().strftime('%Y-%m-%d %H:%M')
                })
                await message.reply_text(f"⚽ <b>GOOOAL! You won {reward} 🎐 shards!</b>", reply_to_message_id=message.id)
            else:
                await message.reply_text("😢 <b>Missed the goal. Try again!</b>", reply_to_message_id=message.id)
        except Exception as e:
            await message.reply_text(f"❌ <b>Error:</b> {e}")

@check_banned
async def dart_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in dart_locks:
        dart_locks[user_id] = asyncio.Lock()
    async with dart_locks[user_id]:
        now = datetime.utcnow()
        last_used = dart_last_used.get(user_id)
        cooldown = dart_cooldowns.get(user_id, COOLDOWN_MIN)
        if last_used and (now - last_used).total_seconds() < cooldown:
            seconds_left = cooldown - int((now - last_used).total_seconds())
            await message.reply_text(f"⏳ Please wait {seconds_left} seconds before using this command again.")
            return
        dart_last_used[user_id] = now
        dart_cooldowns[user_id] = random.randint(COOLDOWN_MIN, COOLDOWN_MAX)
        try:
            if message.from_user is None or message.from_user.is_bot:
                await message.reply_text("❌ <b>Only real users can play this game!</b>")
                return
            dice = await client.send_dice(
                chat_id=message.chat.id,
                emoji="🎯"
            )
            if not hasattr(dice, 'dice') or dice.dice is None:
                await message.reply_text("❌ <b>Failed to send dart. Please try again later.</b>")
                return
            await asyncio.sleep(3)
            if dice.dice.value == 6:
                reward = random.randint(100, 600)
                db = get_database()
                user = await db.get_user(user_id)
                if not user:
                    await message.reply_text("❌ <b>You need an account to receive rewards. Use /start first!</b>")
                    return
                shards = user.get('shards', 0)
                await db.update_user(user_id, {'shards': shards + reward})
                await db.log_user_transaction(user_id, "dart_win", {
                    "reward": reward,
                    "chat_id": message.chat.id,
                    "date": datetime.utcnow().strftime('%Y-%m-%d %H:%M')
                })
                await message.reply_text(f"🎯 <b>Bullseye! You won {reward} 🎐 shards!</b>", reply_to_message_id=message.id)
            else:
                await message.reply_text("😢 <b>Missed the bullseye. Try again!</b>", reply_to_message_id=message.id)
        except Exception as e:
            await message.reply_text(f"❌ <b>Error:</b> {e}")

@check_banned
async def basket_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in basket_locks:
        basket_locks[user_id] = asyncio.Lock()
    async with basket_locks[user_id]:
        now = datetime.utcnow()
        last_used = basket_last_used.get(user_id)
        cooldown = basket_cooldowns.get(user_id, COOLDOWN_MIN)
        if last_used and (now - last_used).total_seconds() < cooldown:
            seconds_left = cooldown - int((now - last_used).total_seconds())
            await message.reply_text(f"⏳ Please wait {seconds_left} seconds before using this command again.")
            return
        basket_last_used[user_id] = now
        basket_cooldowns[user_id] = random.randint(COOLDOWN_MIN, COOLDOWN_MAX)
        try:
            if message.from_user is None or message.from_user.is_bot:
                await message.reply_text("❌ <b>Only real users can play this game!</b>")
                return
            dice = await client.send_dice(
                chat_id=message.chat.id,
                emoji="🏀"
            )
            if not hasattr(dice, 'dice') or dice.dice is None:
                await message.reply_text("❌ <b>Failed to send basketball. Please try again later.</b>")
                return
            await asyncio.sleep(3)
            if dice.dice.value in (4, 5):
                reward = random.randint(100, 600)
                db = get_database()
                user = await db.get_user(user_id)
                if not user:
                    await message.reply_text("❌ <b>You need an account to receive rewards. Use /start first!</b>")
                    return
                shards = user.get('shards', 0)
                await db.update_user(user_id, {'shards': shards + reward})
                await db.log_user_transaction(user_id, "basket_win", {
                    "reward": reward,
                    "chat_id": message.chat.id,
                    "date": datetime.utcnow().strftime('%Y-%m-%d %H:%M')
                })
                await message.reply_text(f"🏀 <b>Nice shot! You won {reward} 🎐 shards!</b>", reply_to_message_id=message.id)
            else:
                await message.reply_text("😢 <b>Missed the basket. Try again!</b>", reply_to_message_id=message.id)
        except Exception as e:
            await message.reply_text(f"❌ <b>Error:</b> {e}")

@check_banned
async def roll_command(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in roll_locks:
        roll_locks[user_id] = asyncio.Lock()
    async with roll_locks[user_id]:
        now = datetime.utcnow()
        last_used = roll_last_used.get(user_id)
        cooldown = roll_cooldowns.get(user_id, COOLDOWN_MIN)
        if last_used and (now - last_used).total_seconds() < cooldown:
            seconds_left = cooldown - int((now - last_used).total_seconds())
            await message.reply_text(f"⏳ Please wait {seconds_left} seconds before using this command again.")
            return
        roll_last_used[user_id] = now
        roll_cooldowns[user_id] = random.randint(COOLDOWN_MIN, COOLDOWN_MAX)
        try:
            args = message.text.split()
            if len(args) < 2:
                await message.reply_text("<b>ℹ️ Please specify a number between 1 and 6, like this: /roll 5</b>")
                return
            if message.from_user is None or message.from_user.is_bot:
                await message.reply_text("❌ <b>Only real users can play this game!</b>")
                return
            try:
                user_number = int(args[1])
            except ValueError:
                await message.reply_text("❌ <b>Please provide a valid number between 1 and 6.</b>")
                return
            if not (1 <= user_number <= 6):
                await message.reply_text("❌ <b>Please provide a number between 1 and 6.</b>")
                return
            dice = await client.send_dice(
                chat_id=message.chat.id,
                emoji="🎲"
            )
            if not hasattr(dice, 'dice') or dice.dice is None:
                await message.reply_text("❌ <b>Failed to roll the dice. Please try again later.</b>")
                return
            await asyncio.sleep(3)
            rolled = dice.dice.value
            if rolled == user_number:
                reward = random.randint(100, 600)
                db = get_database()
                user = await db.get_user(user_id)
                if not user:
                    await message.reply_text("❌ <b>You need an account to receive rewards. Use /start first!</b>")
                    return
                shards = user.get('shards', 0)
                await db.update_user(user_id, {'shards': shards + reward})
                await message.reply_text(f"🎲 <b>Congrats! You guessed {user_number} and rolled {rolled}!</b>\nYou won <b>{reward} 🎐 shards!</b>", reply_to_message_id=message.id)
            else:
                await message.reply_text(f"🎲 <b>You guessed {user_number}, but rolled {rolled}. No reward this time!</b>", reply_to_message_id=message.id)
        except Exception as e:
            await message.reply_text(f"❌ <b>Error:</b> {e}")

            
          