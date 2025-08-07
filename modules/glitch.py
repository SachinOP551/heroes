from telethon import TelegramClient
import asyncio
import time

# Replace with your actual credentials
api_id = 20118977      # Replace with your actual api_id
api_hash = "c88e99dd46c405f7357acef8ccc92f85"  # Replace with your actual api_hash

# Group ID and target user
pay_group_id = -1002706845148  # Group where both commands will be sent
target_user_id = 7921822971

# Commands to send
gift_cmd = "/massgift@DreamCollectorBot 274 725 822 80 25 2 194 82 822"
sell_cmd = "/masssell@DreamCollectorBot 274 725 822 80 25 2 194 82 822"

client = TelegramClient("sync_sender_session", api_id, api_hash)

async def main():
    await client.start()

    user_msg = None
    async for msg in client.iter_messages(pay_group_id, limit=30):
        if msg.sender_id == target_user_id:
            user_msg = msg
            break

    if not user_msg:
        print("❌ No recent message from the target user found in pay group.")
        await client.disconnect()
        return

    # Start timing as close as possible to sending
    start_time = time.time()
    gift_task = asyncio.create_task(client.send_message(pay_group_id, gift_cmd, reply_to=user_msg.id))
    sell_task = asyncio.create_task(client.send_message(pay_group_id, sell_cmd))
    gift_msg, sell_msg = await asyncio.gather(gift_task, sell_task)
    end_time = time.time()

    print(f"⚡ Messages sent in {((end_time - start_time) * 1000):.2f}ms")
    print(f"✅ Sent /gift in reply to msg ID {user_msg.id} (msg ID: {gift_msg.id}) in group {pay_group_id}")
    print(f"✅ Sent /sell (msg ID: {sell_msg.id}) in group {pay_group_id}")

    await client.disconnect()

asyncio.run(main())
